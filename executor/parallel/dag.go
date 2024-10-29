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
}

func newDependencyDag() *dependencyDag {
	return &dependencyDag{nodes: make(map[int]*dependencyNode)}
}

func (dag *dependencyDag) add(node *dependencyNode) {
	dag.nodes[node.Id] = node
}

func (dag *dependencyDag) lookup(id int) *dependencyNode {
	return dag.nodes[id]
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
