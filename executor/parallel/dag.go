package parallel

import (
	"fmt"
	"slices"
	"strings"
	"sync"
)

// DependencyDag is a Directed Acyclic Graph that is not necessarily connected.
type DependencyDag struct {
	nodes []*ExecutionNode
	// dependantsById tracks a list of dependant nodes (reverse Dependencies) for every node ID
	dependantsById map[int]map[int]bool
	// dependenciesById tracks a list of dependency nodes for every node ID
	dependenciesById map[int]map[int]bool
	mu               sync.RWMutex
}

// NewDependencyDag computes a dependency DAG given nodes
func NewDependencyDag(nodes []*ExecutionNode) *DependencyDag {
	dag := &DependencyDag{
		nodes:            make([]*ExecutionNode, len(nodes)),
		dependantsById:   make(map[int]map[int]bool),
		dependenciesById: make(map[int]map[int]bool),
	}

	for _, node := range nodes {
		dag.nodes[node.SeqId] = node
	}

	dag.computeEdges()

	return dag
}

func (dag *DependencyDag) computeEdges() {
	// Reset the maps or out-of-date values could be left there unintentionally
	dag.dependenciesById = make(map[int]map[int]bool)
	dag.dependantsById = make(map[int]map[int]bool)

	seqIdsByRead := make(map[string]map[int]bool)
	seqIdsByUpdate := make(map[string]map[int]bool)

	orderedNodes := make([]*ExecutionNode, 0, len(dag.nodes))
	for _, node := range dag.nodes {
		orderedNodes = append(orderedNodes, node)
	}
	sortNodesBySeqId(orderedNodes)

	for _, node := range orderedNodes {
		dependencies := make(map[int]bool)
		for _, read := range node.Reads {
			for seqId := range seqIdsByUpdate[read] {
				dependencies[seqId] = true
			}
		}
		for _, update := range node.Updates {
			for seqId := range seqIdsByRead[update.Name] {
				dependencies[seqId] = true
			}
		}
		dag.dependenciesById[node.SeqId] = dependencies

		for depSeqId := range dependencies {
			_, ok := dag.dependantsById[depSeqId]
			if !ok {
				dag.dependantsById[depSeqId] = make(map[int]bool)
			}
			dag.dependantsById[depSeqId][node.SeqId] = true
		}

		for _, update := range node.Updates {
			_, ok := seqIdsByUpdate[update.Name]
			if !ok {
				seqIdsByUpdate[update.Name] = make(map[int]bool)
			}
			seqIdsByUpdate[update.Name][node.SeqId] = true
		}

		for _, read := range node.Reads {
			_, ok := seqIdsByRead[read]
			if !ok {
				seqIdsByRead[read] = make(map[int]bool)
			}
			seqIdsByRead[read][node.SeqId] = true
		}
	}
}

func (dag *DependencyDag) NodeIds() []int {
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	result := make([]int, 0, len(dag.nodes))
	for _, node := range dag.nodes {
		result = append(result, node.SeqId)
	}
	return result
}

func (dag *DependencyDag) Node(seqId int) *ExecutionNode {
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	return dag.nodes[seqId]
}

func (dag *DependencyDag) Dependants(seqId int) []int {
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	var seqIds []int
	for id := range dag.dependantsById[seqId] {
		seqIds = append(seqIds, id)
	}
	return seqIds
}

func (dag *DependencyDag) Dependencies(seqId int) []int {
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	var seqIds []int
	for id := range dag.dependenciesById[seqId] {
		seqIds = append(seqIds, id)
	}
	return seqIds
}

type updateDiff struct {
	dependants   sliceDiff
	dependencies sliceDiff
}

func (d updateDiff) String() string {
	return fmt.Sprintf("updateDiff{dependants: %v, dependencies: %v}", d.dependants, d.dependencies)
}

type sliceDiff struct {
	added   []int
	removed []int
}

func (d sliceDiff) String() string {
	return fmt.Sprintf("sliceDiff{added: %v, removed: %v}", d.added, d.removed)
}

func computeSliceDiff(before, after []int) sliceDiff {
	return sliceDiff{
		added:   subtract(after, before),
		removed: subtract(before, after),
	}
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

// update the DAG (if needed) with the re-executed execution node
// returns the newly added Dependencies/Dependants for the new node
func (dag *DependencyDag) update(newNode *ExecutionNode) updateDiff {
	oldNode := dag.nodes[newNode.SeqId]

	if oldNode == nil {
		panic(fmt.Sprintf("cannot call update for new node with SeqId %d", newNode.SeqId))
	}

	beforeDependants := dag.Dependants(newNode.SeqId)
	beforeDependencies := dag.Dependencies(newNode.SeqId)

	dag.mu.Lock()
	dag.nodes[newNode.SeqId] = newNode
	// TODO(perf): Incrementally (without rebuilding the whole graph) compute edges?
	dag.computeEdges()
	dag.mu.Unlock()

	afterDependants := dag.Dependants(newNode.SeqId)
	afterDependencies := dag.Dependencies(newNode.SeqId)

	return updateDiff{
		dependants:   computeSliceDiff(beforeDependants, afterDependants),
		dependencies: computeSliceDiff(beforeDependencies, afterDependencies),
	}
}

func (dag *DependencyDag) Execute(txExecutor *transactionExecutor, state *accountDelta, nWorkers int) {
	queue := newChannelProcessingQueue()
	executor := newDagExecutor(dag, queue)

	for workerId := 0; workerId < nWorkers; workerId++ {
		go func(q <-chan processingTask) {
			for task := range q {
				node := dag.Node(task.seqId)

				if !task.isStale || len(node.Reads) == 0 {
					state.ApplyUpdates(node.SeqId, node.Updates)
					task.done()
					return
				}

				reExecutedNode := txExecutor.execute(state, node.SeqId, *node.Transaction)

				diff := dag.update(reExecutedNode)

				// The new Updates may affect new nodes in the DAG,
				// so we must revert the state update and reprocess at a later point to compute the correct state Updates.
				// Invalidate entire descendants subgraph,
				// because all the succeeding state Updates may be invalid as well.
				dag.depthFirstSearch(diff.dependants.added, func(seqId int) {
					state.RevertUpdates(seqId, dag.Node(seqId).Updates)
					executor.markUnvisited(seqId)
				})

				dag.depthFirstSearch(diff.dependants.removed, func(seqId int) {
					state.RevertUpdates(seqId, dag.Node(seqId).Updates)
					executor.markUnvisited(seqId)
					// This node may not be reachable from the current subgraph,
					// so we need a way to tell the executor to re-execute it
					// alongside re-scheduling its processing (marking it unvisited).
					executor.markStale(seqId)
				})

				if len(diff.dependencies.added) == 0 {
					state.ApplyUpdates(reExecutedNode.SeqId, reExecutedNode.Updates)
				} else {
					// This node was moved to a different part of the DAG
					// and should be processed again at a later point.
					executor.markUnvisited(reExecutedNode.SeqId)
					for _, seqId := range diff.dependencies.added {
						executor.markUnvisited(seqId)
					}
					state.RevertUpdates(node.SeqId, node.Updates)
				}

				task.done()
			}
		}(queue.tasks())
	}

	executor.execute()
}

func (dag *DependencyDag) depthFirstSearch(fromSeqIds []int, callback func(int)) {
	q := make([]int, 0)
	q = append(q, fromSeqIds...)
	for len(q) > 0 {
		seqId := q[0]
		q = q[1:]

		callback(seqId)

		// TODO(perf): Only push the ones that weren't visited yet for the invalidation traversal
		q = append(q, dag.Dependants(seqId)...)
	}
}

func (dag *DependencyDag) Graphviz() string {
	graphViz := Graphviz{
		Name:             "DependencyGraph",
		OutlinedNodes:    map[int]bool{},
		NodeFillColor:    NewRgbColor(142, 202, 230),
		NodeLabelColor:   NewRgbColor(2, 48, 71),
		NodeOutlineColor: NewRgbColor(255, 183, 3),
		EdgeLabelColor:   NewRgbColor(33, 158, 188),
		EdgeFillColor:    NewRgbColor(2, 48, 71),
		Dag:              dag,
	}
	return graphViz.Generate()
}

func (dag *DependencyDag) String() string {
	sb := strings.Builder{}
	sortedNodes := make([]*ExecutionNode, 0, len(dag.nodes))
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
		d := dag.Dependencies(node.SeqId)
		slices.Sort(d)
		sb.WriteString(fmt.Sprintf("%d -> %v\n", node.SeqId, d))
	}

	sb.WriteString("dependants:\n")
	for _, node := range sortedNodes {
		d := dag.Dependants(node.SeqId)
		slices.Sort(d)
		sb.WriteString(fmt.Sprintf("%d -> %v\n", node.SeqId, d))
	}

	return sb.String()
}

// channelProcessingQueue is the default implementation of processingQueue using channels
type channelProcessingQueue struct {
	queue chan processingTask
	log   [][]processingTask
}

func newChannelProcessingQueue() *channelProcessingQueue {
	return &channelProcessingQueue{queue: make(chan processingTask)}
}

func (q *channelProcessingQueue) enqueue(units []processingTask) {
	for _, unit := range units {
		q.queue <- unit
	}
	q.log = append(q.log, units)
}

func (q *channelProcessingQueue) tasks() <-chan processingTask {
	return q.queue
}

func (q *channelProcessingQueue) close() {
	close(q.queue)
}

func (q *channelProcessingQueue) String() string {
	out := strings.Builder{}
	out.WriteString("[")
	for i, batch := range q.log {
		out.WriteString("[")
		for j, e := range batch {
			out.WriteString(fmt.Sprintf("%d", e.seqId))
			if j != len(batch)-1 {
				out.WriteString(",")
			} else {
				out.WriteString("]")
			}
		}
		if i != len(q.log)-1 {
			out.WriteString(",")
		} else {
			out.WriteString("]")
		}
	}
	return out.String()
}

func sortNodesBySeqId(nodes []*ExecutionNode) {
	slices.SortFunc(nodes, func(a, b *ExecutionNode) int {
		if a.SeqId < b.SeqId {
			return -1
		} else if a.SeqId > b.SeqId {
			return 1
		}
		return 0
	})
}
