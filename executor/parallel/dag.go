package parallel

import (
	"blockchain/executor/api"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// dependencyDag is a Directed Acyclic Graph that is not necessarily connected.
type dependencyDag struct {
	nodes map[int]*executionNode
	// dependantsById tracks a list of dependant nodes (reverse dependencies) for every node ID
	dependantsById map[int]map[int]bool
	// dependenciesById tracks a list of dependency nodes for every node ID
	dependenciesById map[int]map[int]bool
	// mu protects writes/reads to above maps
	mu sync.RWMutex
}

// buildDependencyDag produces a dependency DAG (Directed Acyclic Graph)
// given an ordered (by nodeSeqId) list of execution reports
func newDependencyDag(nodes []*executionNode) *dependencyDag {
	dag := &dependencyDag{
		nodes:            make(map[int]*executionNode),
		dependantsById:   make(map[int]map[int]bool),
		dependenciesById: make(map[int]map[int]bool),
	}

	for _, node := range nodes {
		dag.nodes[node.seqId] = node
	}

	dag.computeEdges()

	return dag
}

func (dag *dependencyDag) computeEdges() {
	seqIdsByRead := make(map[string]map[int]bool)
	seqIdsByUpdate := make(map[string]map[int]bool)

	orderedNodes := make([]*executionNode, 0, len(dag.nodes))
	for _, node := range dag.nodes {
		orderedNodes = append(orderedNodes, node)
	}
	slices.SortFunc(orderedNodes, func(a, b *executionNode) int {
		return a.seqId - b.seqId
	})

	for _, node := range orderedNodes {
		dependencies := make(map[int]bool)
		for _, read := range node.reads {
			for seqId, _ := range seqIdsByUpdate[read] {
				dependencies[seqId] = true
			}
		}
		for _, update := range node.updates {
			for seqId, _ := range seqIdsByRead[update.Name] {
				dependencies[seqId] = true
			}
		}
		dag.dependenciesById[node.seqId] = dependencies

		for depSeqId, _ := range dependencies {
			_, ok := dag.dependantsById[depSeqId]
			if !ok {
				dag.dependantsById[depSeqId] = make(map[int]bool)
			}
			dag.dependantsById[depSeqId][node.seqId] = true
		}

		dag.nodes[node.seqId] = node

		for _, update := range node.updates {
			_, ok := seqIdsByUpdate[update.Name]
			if !ok {
				seqIdsByUpdate[update.Name] = make(map[int]bool)
			}
			seqIdsByUpdate[update.Name][node.seqId] = true
		}

		for _, read := range node.reads {
			_, ok := seqIdsByRead[read]
			if !ok {
				seqIdsByRead[read] = make(map[int]bool)
			}
			seqIdsByRead[read][node.seqId] = true
		}
	}
}

func (dag *dependencyDag) lookup(id int) *executionNode {
	// TODO: Move locking responsibility to the caller?
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	return dag.nodes[id]
}

func (dag *dependencyDag) dependants(seqId int) []int {
	var seqIds []int

	for id, _ := range dag.dependantsById[seqId] {
		seqIds = append(seqIds, id)
	}

	return seqIds
}

func (dag *dependencyDag) dependencies(seqId int) []int {
	var seqIds []int

	for id, _ := range dag.dependenciesById[seqId] {
		seqIds = append(seqIds, id)
	}

	return seqIds
}

type updateDiff struct {
	newDependants   []int
	newDependencies []int
}

// update the DAG (if needed) with the re-executed execution node
// returns the newly added dependencies/dependants for the new node
func (dag *dependencyDag) update(newNode *executionNode) updateDiff {
	dag.mu.Lock()
	defer dag.mu.Unlock()

	oldNode := dag.nodes[newNode.seqId]

	if oldNode == nil {
		panic("cannot call update for new nodes")
	}

	dag.nodes[newNode.seqId] = newNode

	beforeDependants := dag.dependants(newNode.seqId)
	beforeDependencies := dag.dependencies(newNode.seqId)

	// TODO(perf): Incrementally (without rebuilding the whole graph) compute edges
	dag.computeEdges()

	afterDependants := dag.dependants(newNode.seqId)
	afterDependencies := dag.dependencies(newNode.seqId)

	return updateDiff{
		newDependants:   subtract(afterDependants, beforeDependants),
		newDependencies: subtract(afterDependencies, beforeDependencies),
	}
}

type processingQueue interface {
	process([]processingTask)
	close()
}

type channelProcessingQueue struct {
	queue chan processingTask
}

func newChannelProcessingQueue() *channelProcessingQueue {
	return &channelProcessingQueue{queue: make(chan processingTask)}
}

func (q *channelProcessingQueue) process(units []processingTask) {
	for _, unit := range units {
		q.queue <- unit
	}
}

func (q *channelProcessingQueue) close() {
	close(q.queue)
}

type processingTask struct {
	nodeSeqId     int
	done          func()
	markUnvisited func(seqId int)
}

func (dag *dependencyDag) execute(state *executionAccountState, nWorkers int) {
	queue := newChannelProcessingQueue()

	for i := 0; i < nWorkers; i++ {
		go func(workerId int, queue <-chan processingTask) {
			for task := range queue {
				node := dag.lookup(task.nodeSeqId)

				reExecutedNode := executeTransaction(state, node.seqId, *node.transaction)

				diff := dag.update(reExecutedNode)

				for _, seqId := range diff.newDependants {
					newDependant := dag.lookup(seqId)
					state.RevertUpdates(newDependant.updates)

					task.markUnvisited(seqId)
					// TODO: Do we need to mark all visited descendant nodes as unvisited as well?
				}

				if len(diff.newDependencies) == 0 {
					state.ApplyUpdates(reExecutedNode.updates)
				} else {
					task.markUnvisited(reExecutedNode.seqId)
				}

				task.done()
			}
		}(i, queue.queue)
	}

	dag.concurrentWalk(queue)
}

// TODO(cleanup): Refactor this into a DagWalker struct
func (dag *dependencyDag) concurrentWalk(queue processingQueue) {
	// nodes in the queue were already processed
	// and are waiting to be traversed to process their dependants
	q := make([]int, 0)
	wg := sync.WaitGroup{}
	batch := make([]processingTask, 0)
	visited := make([]bool, len(dag.nodes))

	unvisitedDependencies := func(seqId int) []int {
		unvisited := make([]int, 0)
		for _, depSeqId := range dag.dependencies(seqId) {
			if !visited[depSeqId] {
				unvisited = append(unvisited, depSeqId)
			}
		}
		return unvisited
	}
	unvisitedDependants := func(seqId int) []int {
		unvisited := make([]int, 0)
		for _, depSeqId := range dag.dependants(seqId) {
			if !visited[depSeqId] {
				unvisited = append(unvisited, depSeqId)
			}
		}
		return unvisited
	}
	schedule := func(seqId int) {
		wg.Add(1)
		batch = append(batch, processingTask{
			nodeSeqId: seqId,
			done: func() {
				wg.Done()
			},
			markUnvisited: func(seqId int) {
				// TODO(verify): Is this true?
				// Can be called concurrently for different nodeSeqId values
				visited[seqId] = false
			},
		})
		q = append(q, seqId)
	}
	awaitProcessing := func() {
		if len(batch) > 0 {
			queue.process(batch)
			batch = make([]processingTask, 0)
			wg.Wait()
		}
	}

	for seqId, _ := range dag.nodes {
		if len(dag.dependencies(seqId)) == 0 {
			schedule(seqId)
		}
	}

	awaitProcessing()

	for len(q) > 0 {

		n := len(q)
		for i := 0; i < n; i++ {
			seqId := q[i]

			// Dependencies changed since last processing, delay removal.
			// Node will be processed again at some future point,
			// since it now depends on some other node that has yet to be traversed.
			if len(unvisitedDependencies(seqId)) > 0 {
				continue
			}

			dependants := unvisitedDependants(seqId)
			visited[seqId] = true

			for _, depSeqId := range dependants {
				if len(unvisitedDependencies(depSeqId)) == 0 {
					schedule(depSeqId)
				}
			}
		}
		q = q[n:]

		awaitProcessing()
	}

	queue.close()
}

func (dag *dependencyDag) String() string {
	sb := strings.Builder{}
	sortedNodes := make([]*executionNode, 0, len(dag.nodes))
	for _, node := range dag.nodes {
		sortedNodes = append(sortedNodes, node)
	}
	slices.SortFunc(sortedNodes, func(a, b *executionNode) int {
		return a.seqId - b.seqId
	})

	sb.WriteString("nodes:\n")
	for _, node := range sortedNodes {
		sb.WriteString(fmt.Sprintf("- %s\n", node.String()))
	}

	sb.WriteString("dependencies:\n")
	for _, node := range sortedNodes {
		d := dag.dependencies(node.seqId)
		slices.Sort(d)
		sb.WriteString(fmt.Sprintf("%d -> %v\n", node.seqId, d))
	}

	sb.WriteString("dependants:\n")
	for _, node := range sortedNodes {
		d := dag.dependants(node.seqId)
		slices.Sort(d)
		sb.WriteString(fmt.Sprintf("%d -> %v\n", node.seqId, d))
	}

	return sb.String()
}

type executionNode struct {
	// seqId is a unique identifier that also indicates the transaction order within a block
	seqId int
	// reads account names that were read by the transaction
	reads []string
	// updates account state changes that were produced by the transaction
	updates []api.AccountUpdate
	// no updates were produced if err is set
	err error
	// transaction is the state transition function that produces reads, updates, err
	transaction *api.Transaction
}

func (node *executionNode) String() string {
	readNames := make([]string, len(node.reads))
	for i, read := range node.reads {
		readNames[i] = read
	}

	updateEntries := make([]string, len(node.updates))
	for i, update := range node.updates {
		updateEntries[i] = update.Name + ":" + strconv.FormatInt(int64(update.BalanceChange), 10)
	}

	return fmt.Sprintf(
		"executionNode{nodeSeqId:%d,reads:%s,updates:%s,err:%v}",
		node.seqId,
		"("+strings.Join(readNames, ",")+")",
		"("+strings.Join(updateEntries, ",")+")",
		node.err,
	)
}

// subtract returns integers that are in first but not second array
func subtract(first, second []int) []int {
	var sub []int
outer:
	for _, a := range first {
		for _, b := range second {
			if a == b {
				continue outer
			}
		}
		sub = append(sub, a)
	}
	return sub
}
