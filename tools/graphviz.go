package tools

import (
	"blockchain/executor/parallel"
	"blockchain/executor/types"
	"fmt"
	"math/rand"
	"strings"
)

// Graphviz outputs a graph in DOT graphing language that can be viewed in supporting visualisation program.
// See: https://graphviz.org/doc/info/lang.html
// Online viewer: https://dreampuf.github.io/GraphvizOnline
type Graphviz struct {
	Name string
	// Seq IDs of outlined nodes
	OutlinedNodes map[int]bool
	Dag           *parallel.DependencyDag
}

func (g *Graphviz) Generate() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("digraph %s {\n", g.Name))

	color := randomHexColor()
	sb.WriteString(fmt.Sprintf("\tnode [style=filled,color=\"%s\"];\n", color))
	for _, seqId := range g.Dag.NodeSeqIds() {
		node := g.Dag.Get(seqId)
		if g.OutlinedNodes[seqId] {
			sb.WriteString(fmt.Sprintf("\t%s [color=\"#ffa500\", penwidth=3, fillcolor=\"%s\", style=\"filled\"];\n", g.nodeName(seqId), color))
		}
		if len(g.Dag.Dependants(seqId)) == 0 && len(g.Dag.Dependencies(seqId)) == 0 {
			sb.WriteString(fmt.Sprintf("\t%s;\n", g.nodeName(seqId)))
			continue
		}
		for _, depSeqId := range g.Dag.Dependants(seqId) {
			depNode := g.Dag.Get(depSeqId)
			sb.WriteString(fmt.Sprintf("\t%s -> %s ", g.nodeName(seqId), g.nodeName(depSeqId)))
			sb.WriteString(fmt.Sprintf(
				"[label=\"%s\", fontsize=8, fontcolor=\"#a0a0a0\"];\n",
				conflictingAccounts(node, depNode),
			))
		}
	}
	sb.WriteString("}")
	return sb.String()
}

func (g *Graphviz) nodeName(seqId int) string {
	return fmt.Sprintf("%d", seqId)
}

func randomHexColor() string {
	return fmt.Sprintf(
		"#%s%s%s",
		randomHexColorComponent(),
		randomHexColorComponent(),
		randomHexColorComponent(),
	)
}

func randomHexColorComponent() string {
	// Prefer lighter colors, so that foreground text is easily visible.
	minValue := 180
	value := rand.Intn(255 - minValue)
	hex := fmt.Sprintf("%x", value)
	if len(hex) == 1 {
		hex = "0" + hex
	}
	return hex
}

// conflictingAccounts returns account names that cause the dependency relationship between given nodes.
func conflictingAccounts(dependency, dependant *parallel.ExecutionNode) []string {
	intersections := make(map[string]bool)
	for _, name := range intersect(dependency.Updates, dependant.Reads) {
		intersections[name] = true
	}
	for _, name := range intersect(dependant.Updates, dependency.Reads) {
		intersections[name] = true
	}
	var unique []string
	for name := range intersections {
		unique = append(unique, name)
	}
	return unique
}

func intersect(updates []types.AccountUpdate, reads []string) []string {
	readsLookup := make(map[string]bool, len(reads))
	for _, read := range reads {
		readsLookup[read] = true
	}
	var intersection []string
	for _, update := range updates {
		if readsLookup[update.Name] {
			intersection = append(intersection, update.Name)
		}
	}
	return intersection
}
