package parallel

import (
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
	OutlinedNodes    map[int]bool
	NodeFillColor    Color
	NodeOutlineColor Color
	NodeLabelColor   Color
	EdgeLabelColor   Color
	EdgeFillColor    Color
	GetNodeLabel     func(seqId int) string
}

func (g *Graphviz) Generate(dag *DependencyDag) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("digraph %s {\n", g.Name))
	sb.WriteString(fmt.Sprintf(
		"\tnode [style=filled, color=\"%s\", fontcolor=\"%s\"];\n",
		g.NodeFillColor,
		g.NodeLabelColor,
	))

	for _, seqId := range dag.NodeIds() {
		node := dag.Node(seqId)
		if len(dag.Dependants(seqId)) == 0 && len(dag.Dependencies(seqId)) == 0 {
			sb.WriteString(fmt.Sprintf("\t%s;\n", g.nodeName(seqId)))
		}
		for _, depSeqId := range dag.Dependants(seqId) {
			depNode := dag.Node(depSeqId)
			sb.WriteString(fmt.Sprintf("\t%s -> %s ", g.nodeName(seqId), g.nodeName(depSeqId)))
			sb.WriteString(fmt.Sprintf(
				"[label=\"%s\", fontsize=8, fontcolor=\"%s\", color=\"%s\"];\n",
				conflictingAccounts(node, depNode),
				g.EdgeLabelColor,
				g.EdgeFillColor,
			))
		}
		if g.OutlinedNodes[seqId] {
			sb.WriteString(fmt.Sprintf(
				"\t%s [color=\"%s\", penwidth=3, fillcolor=\"%s\", style=\"filled\"];\n",
				g.nodeName(seqId),
				g.NodeOutlineColor,
				g.NodeFillColor,
			))
		}
	}
	sb.WriteString("}")
	return sb.String()
}

func (g *Graphviz) nodeName(seqId int) string {
	if g.GetNodeLabel == nil {
		return fmt.Sprintf("%d", seqId)
	} else {
		return g.GetNodeLabel(seqId)
	}
}

type Color struct {
	R, G, B uint8
}

func NewRgbColor(r, g, b uint8) Color {
	return Color{r, g, b}
}

func NewRandomColor() Color {
	// Prefer lighter colors, so that foreground text is easily visible.
	minValue := 180
	return Color{
		R: uint8(rand.Intn(255 - minValue)),
		G: uint8(rand.Intn(255 - minValue)),
		B: uint8(rand.Intn(255 - minValue)),
	}
}

func (c Color) String() string {
	return c.hex()
}

func (c Color) hex() string {
	var components = []uint8{
		c.R,
		c.G,
		c.B,
	}
	var sb strings.Builder
	sb.WriteString("#")
	for _, component := range components {
		sb.WriteString(fmt.Sprintf("%02X", component))
	}
	return sb.String()
}

// conflictingAccounts returns account names that cause the dependency relationship between given nodes.
func conflictingAccounts(dependency, dependant *ExecutionNode) []string {
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
