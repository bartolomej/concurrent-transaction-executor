package parallel

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// dependencyDag is a Directed Acyclic Graph that is not necessarily connected.
type dependencyDag struct {
	nodes map[int]*dependencyNode
	// dependantsById tracks a list of dependant nodes (those who depend on the dependency node) for every node ID
	dependantsById map[int]map[int]bool
}

func newDependencyDag(nodes []*dependencyNode) *dependencyDag {
	dag := &dependencyDag{
		nodes:          make(map[int]*dependencyNode),
		dependantsById: make(map[int]map[int]bool),
	}

	for _, node := range nodes {
		dag.add(node)
	}

	return dag
}

// add inserts the node in the graph
// once a node is added is should not be updated or er-added
func (dag *dependencyDag) add(node *dependencyNode) {
	_, exists := dag.nodes[node.Id]
	if exists {
		panic("cannot add the same node twice")
	}

	dag.nodes[node.Id] = node

	dag.updateLookups(node)
}

func (dag *dependencyDag) updateLookups(node *dependencyNode) {
	for _, dep := range node.Dependencies {
		_, ok := dag.dependantsById[dep]
		if !ok {
			dag.dependantsById[dep] = make(map[int]bool)
		}
		dag.dependantsById[dep][node.Id] = true
	}
}

func (dag *dependencyDag) lookup(id int) *dependencyNode {
	return dag.nodes[id]
}

func (dag *dependencyDag) dependants(id int) []int {
	var dependantsIds []int

	for id, _ := range dag.dependantsById[id] {
		dependantsIds = append(dependantsIds, id)
	}

	return dependantsIds
}

type dagQueue interface {
	addBatch([]dagQueueElement)
	close()
}

type dagChannelQueue struct {
	channel chan dagQueueElement
}

func newDagChannelQueue() *dagChannelQueue {
	return &dagChannelQueue{channel: make(chan dagQueueElement)}
}

func (q *dagChannelQueue) addBatch(elements []dagQueueElement) {
	for _, element := range elements {
		q.channel <- element
	}
}

func (q *dagChannelQueue) close() {
	close(q.channel)
}

type dagQueueElement struct {
	nodeId int
	wg     *sync.WaitGroup
}

func (dag *dependencyDag) concurrentWalk(processing dagQueue) {
	q := make([]int, 0)
	inDegreeById := make([]int, len(dag.nodes))
	for id, node := range dag.nodes {
		inDegreeById[id] = len(node.Dependencies)
	}

	wg := sync.WaitGroup{}

	batch := make([]dagQueueElement, 0)
	newElement := func(id int) {
		q = append(q, id)
		wg.Add(1)
		batch = append(batch, dagQueueElement{
			nodeId: id,
			wg:     &wg,
		})
	}
	enqueueBatch := func() {
		processing.addBatch(batch)
		batch = make([]dagQueueElement, 0)
	}

	for id, degree := range inDegreeById {
		if degree == 0 {
			newElement(id)
		}
	}

	enqueueBatch()
	wg.Wait()

	for len(q) > 0 {
		current := q[0]
		q = q[1:]

		for _, d := range dag.dependants(current) {
			inDegreeById[d] = inDegreeById[d] - 1
			if inDegreeById[d] == 0 {
				newElement(d)
			}
		}

		enqueueBatch()
		wg.Wait()
	}

	processing.close()
}

func (dag *dependencyDag) String() string {
	sb := strings.Builder{}
	sortedNodes := make([]*dependencyNode, 0, len(dag.nodes))
	for _, node := range dag.nodes {
		sortedNodes = append(sortedNodes, node)
	}
	slices.SortFunc(sortedNodes, func(a, b *dependencyNode) int {
		return a.Id - b.Id
	})
	for _, node := range sortedNodes {
		sb.WriteString(node.String())
		sb.WriteString("\n")
	}
	return sb.String()
}

type dependencyNode struct {
	Id int
	// Dependencies is a list of dependencies (their IDs)
	Dependencies []int
}

func newDependencyNode(id int, dependencies []int) *dependencyNode {
	return &dependencyNode{
		Id:           id,
		Dependencies: dependencies,
	}
}

func (node dependencyNode) String() string {
	deps := make([]string, len(node.Dependencies))
	for i, dep := range node.Dependencies {
		deps[i] = strconv.Itoa(dep)
	}
	return fmt.Sprintf(
		"Dependency{id:%s,deps:(%s)}",
		strconv.Itoa(node.Id),
		strings.Join(deps, ","),
	)
}
