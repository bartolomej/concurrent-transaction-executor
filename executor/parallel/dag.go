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
	sortNodesBySeqId(orderedNodes)

	for _, node := range orderedNodes {
		dependencies := make(map[int]bool)
		for _, read := range node.reads {
			for seqId := range seqIdsByUpdate[read] {
				dependencies[seqId] = true
			}
		}
		for _, update := range node.updates {
			for seqId := range seqIdsByRead[update.Name] {
				dependencies[seqId] = true
			}
		}
		dag.dependenciesById[node.seqId] = dependencies

		for depSeqId := range dependencies {
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
	return dag.nodes[id]
}

func (dag *dependencyDag) dependants(seqId int) []int {
	var seqIds []int

	for id := range dag.dependantsById[seqId] {
		seqIds = append(seqIds, id)
	}

	return seqIds
}

func (dag *dependencyDag) dependencies(seqId int) []int {
	var seqIds []int

	for id := range dag.dependenciesById[seqId] {
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
		panic(fmt.Sprintf("cannot call update for new node with seqId %d", newNode.seqId))
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
	tasks() <-chan processingTask
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

func (q *channelProcessingQueue) tasks() <-chan processingTask {
	return q.queue
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

				// TODO(perf): Can we sometimes not execute the node again (e.g. when there are no dependencies)?
				reExecutedNode := executeTransaction(state, node.seqId, *node.transaction)

				diff := dag.update(reExecutedNode)

				// The new updates may affect new nodes in the DAG,
				// so we must revert the state update and reprocess at a later point to compute the correct state updates.
				// Invalidate entire descendants subgraph,
				// because all the succeeding state updates may be invalid as well.
				dq := make([]int, 0)
				dq = append(dq, diff.newDependants...)
				for len(dq) > 0 {
					descendantSeqId := dq[0]
					dq = dq[1:]

					descendantNode := dag.lookup(descendantSeqId)
					state.RevertUpdates(descendantNode.updates)
					task.markUnvisited(descendantSeqId)

					dq = append(dq, dag.dependants(descendantSeqId)...)
				}

				if len(diff.newDependencies) == 0 {
					state.ApplyUpdates(reExecutedNode.updates)
				} else {
					// This node was moved to a different part of the DAG
					// and should be processed again at a later point.
					task.markUnvisited(reExecutedNode.seqId)
				}

				task.done()
			}
		}(i, queue.tasks())
	}

	executor := newDagExecutor(dag, queue)
	executor.execute()
}

func (dag *dependencyDag) String() string {
	sb := strings.Builder{}
	sortedNodes := make([]*executionNode, 0, len(dag.nodes))
	for _, node := range dag.nodes {
		sortedNodes = append(sortedNodes, node)
	}
	sortNodesBySeqId(sortedNodes)

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
	m := make(map[int]bool, len(second))
	for _, b := range second {
		m[b] = true
	}
	var sub []int
	for _, a := range first {
		if _, found := m[a]; !found {
			sub = append(sub, a)
		}
	}
	return sub
}

func sortNodesBySeqId(nodes []*executionNode) {
	slices.SortFunc(nodes, func(a, b *executionNode) int {
		if a.seqId < b.seqId {
			return -1
		} else if a.seqId > b.seqId {
			return 1
		}
		return 0
	})
}
