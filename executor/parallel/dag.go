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
// given an ordered (by seqId) list of execution reports
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
	}
}

func (dag *dependencyDag) lookup(id int) *executionNode {
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	return dag.nodes[id]
}

func (dag *dependencyDag) dependants(id int) []int {
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	var seqIds []int

	for id, _ := range dag.dependantsById[id] {
		seqIds = append(seqIds, id)
	}

	return seqIds
}

func (dag *dependencyDag) dependencies(id int) []int {
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	var seqIds []int

	for id, _ := range dag.dependenciesById[id] {
		seqIds = append(seqIds, id)
	}

	return seqIds
}

func (dag *dependencyDag) update(newNode *executionNode) {
	dag.mu.Lock()
	defer dag.mu.Unlock()

	oldNode := dag.nodes[newNode.seqId]

	if oldNode == nil {
		panic("cannot call update for new nodes")
	}

	dag.nodes[newNode.seqId] = newNode

	// TODO: Optimize graph updates by not rebuilding the graph from scratch
	dag.computeEdges()
}

type processingQueue interface {
	process([]processingUnit)
	close()
}

type channelProcessingQueue struct {
	queue chan processingUnit
}

func newChannelProcessingQueue() *channelProcessingQueue {
	return &channelProcessingQueue{queue: make(chan processingUnit)}
}

func (q *channelProcessingQueue) process(units []processingUnit) {
	for _, unit := range units {
		q.queue <- unit
	}
}

func (q *channelProcessingQueue) close() {
	close(q.queue)
}

type processingUnit struct {
	seqId int
	done  func()
}

func (dag *dependencyDag) concurrentWalk(queue processingQueue) {
	// nodes in the queue were already processed
	// and are waiting to be traversed to process their dependants
	q := make([]int, 0)
	wg := sync.WaitGroup{}
	batch := make([]processingUnit, 0)

	schedule := func(seqId int) {
		wg.Add(1)
		batch = append(batch, processingUnit{
			seqId: seqId,
			done: func() {
				wg.Done()
			},
		})
		q = append(q, seqId)
	}
	awaitProcessing := func() {
		if len(batch) > 0 {
			queue.process(batch)
			batch = make([]processingUnit, 0)
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
			if len(dag.dependencies(seqId)) > 0 {
				continue
			}

			dependants := dag.dependants(seqId)
			dag.remove(seqId)

			for _, depSeqId := range dependants {
				if len(dag.dependencies(depSeqId)) == 0 {
					schedule(depSeqId)
				}
			}
		}
		q = q[n:]

		awaitProcessing()
	}

	queue.close()
}

// remove the node and it's edges from the graph
func (dag *dependencyDag) remove(seqId int) {
	dag.mu.Lock()
	defer dag.mu.Unlock()

	for depSeqId := range dag.dependantsById[seqId] {
		delete(dag.dependenciesById[depSeqId], seqId)
	}
	for depSeqId := range dag.dependenciesById[seqId] {
		delete(dag.dependantsById[depSeqId], seqId)
	}

	delete(dag.dependenciesById, seqId)
	delete(dag.dependantsById, seqId)
	delete(dag.nodes, seqId)
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
		"executionNode{seqId:%d,reads:%s,updates:%s,err:%v}",
		node.seqId,
		"("+strings.Join(readNames, ",")+")",
		"("+strings.Join(updateEntries, ",")+")",
		node.err,
	)
}
