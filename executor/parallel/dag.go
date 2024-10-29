package parallel

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// dependencyDag is a Directed Acyclic Graph that is not necessarily connected.
type dependencyDag struct {
	nodes map[int]*dependencyNode
	// inEdgeLookup tracks incoming node connections by node ID
	inEdgeLookup map[int]map[int]bool
}

func newDependencyDag(nodes []*dependencyNode) *dependencyDag {
	dag := &dependencyDag{
		nodes:        make(map[int]*dependencyNode),
		inEdgeLookup: make(map[int]map[int]bool),
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
		_, ok := dag.inEdgeLookup[dep]
		if !ok {
			dag.inEdgeLookup[dep] = make(map[int]bool)
		}
		dag.inEdgeLookup[dep][node.Id] = true
	}
}

func (dag *dependencyDag) lookup(id int) *dependencyNode {
	return dag.nodes[id]
}

func (dag *dependencyDag) dependants(id int) []int {
	var dependantsIds []int

	for id, _ := range dag.inEdgeLookup[id] {
		dependantsIds = append(dependantsIds, id)
	}

	return dependantsIds
}

func (dag *dependencyDag) topologicalOrder() []int {
	var orderedIds []int
	visitedIds := make(map[int]bool)

	// Find all nodes without dependencies
	for id, node := range dag.nodes {
		if len(node.Dependencies) == 0 {
			orderedIds = append(orderedIds, id)
			visitedIds[id] = true
		}
	}

	i := 0
	for i < len(orderedIds) {
		for _, depId := range dag.dependants(orderedIds[i]) {
			if !visitedIds[depId] {
				orderedIds = append(orderedIds, depId)
				visitedIds[depId] = true
			}
		}
		i++
	}

	return orderedIds
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
