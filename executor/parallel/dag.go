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
	// dependantsById tracks a list of dependant nodes (reverse Dependencies) for every node seq ID
	dependantsById map[int]map[int]bool
	// dependenciesById tracks a list of dependency nodes for every node seq ID
	dependenciesById map[int]map[int]bool
	// Mutex controls access to the above maps.
	mu sync.RWMutex
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
		// Computes dependencies for the current node.
		// A dependency is a transaction node with lower seq ID that either:
		// - updates an account the current node reads from,
		//   so it must be executed before the current node.
		// - reads the account the current node updates,
		//   so the current node must be executed after.
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

		// Update the reverse mapping.
		for depSeqId := range dependencies {
			addToMultiSet(dag.dependantsById, depSeqId, node.SeqId)
		}

		// Update temporary lookups.
		for _, update := range node.Updates {
			addToMultiSet(seqIdsByUpdate, update.Name, node.SeqId)
		}
		for _, read := range node.Reads {
			addToMultiSet(seqIdsByRead, read, node.SeqId)
		}

		dag.dependenciesById[node.SeqId] = dependencies
	}
}

// addToMultiSet adds the innerKey to the inner set under outerKey
// while also ensuring that the inner set (map) is allocated.
func addToMultiSet[K comparable](lookup map[K]map[int]bool, outerKey K, innerKey int) {
	_, ok := lookup[outerKey]
	if !ok {
		lookup[outerKey] = make(map[int]bool)
	}
	lookup[outerKey][innerKey] = true
}

// NodeIds returns seq IDs for all nodes in the graph
func (dag *DependencyDag) NodeIds() []int {
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	result := make([]int, 0, len(dag.nodes))
	for _, node := range dag.nodes {
		result = append(result, node.SeqId)
	}
	return result
}

// Node returns a reference to the ExecutionNode in the graph given seq ID
func (dag *DependencyDag) Node(seqId int) *ExecutionNode {
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	return dag.nodes[seqId]
}

// Dependants returns all nodes (their seq IDs) that depend on the given seq ID
func (dag *DependencyDag) Dependants(seqId int) []int {
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	var seqIds []int
	for id := range dag.dependantsById[seqId] {
		seqIds = append(seqIds, id)
	}
	return seqIds
}

// Dependencies returns all nodes (their seq IDs) that the given seqId depends on
func (dag *DependencyDag) Dependencies(seqId int) []int {
	dag.mu.RLock()
	defer dag.mu.RUnlock()

	var seqIds []int
	for id := range dag.dependenciesById[seqId] {
		seqIds = append(seqIds, id)
	}
	return seqIds
}

// updateDiff records the DAG changes for a single node update
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
	dag.computeEdges()
	dag.mu.Unlock()

	afterDependants := dag.Dependants(newNode.SeqId)
	afterDependencies := dag.Dependencies(newNode.SeqId)

	return updateDiff{
		dependants:   computeSliceDiff(beforeDependants, afterDependants),
		dependencies: computeSliceDiff(beforeDependencies, afterDependencies),
	}
}

// bfsFrom traverses the DAG with breadth first search from startSeqIds
func (dag *DependencyDag) bfsFrom(startSeqIds []int, shouldDescend func(int) bool) {
	q := make([]int, 0)
	q = append(q, startSeqIds...)
	for len(q) > 0 {
		seqId := q[0]
		q = q[1:]

		if shouldDescend(seqId) {
			q = append(q, dag.Dependants(seqId)...)
		}
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
	}
	return graphViz.Generate(dag)
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

// sortNodesBySeqId sorts nodes by seq ID in ascending order
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
