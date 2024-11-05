package parallel

import (
	"blockchain/executor/api"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// dependencyDag is a Directed Acyclic Graph that is not necessarily connected.
type dependencyDag struct {
	nodes []*executionNode
	// dependantsById tracks a list of dependant nodes (reverse dependencies) for every node ID
	dependantsById map[int]map[int]bool
	// dependenciesById tracks a list of dependency nodes for every node ID
	dependenciesById map[int]map[int]bool
}

// buildDependencyDag computes a dependency DAG given nodes
func newDependencyDag(nodes []*executionNode) *dependencyDag {
	dag := &dependencyDag{
		nodes:            make([]*executionNode, len(nodes)),
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
	// Reset the maps or out-of-date values could be left there unintentionally
	dag.dependenciesById = make(map[int]map[int]bool)
	dag.dependantsById = make(map[int]map[int]bool)

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

func (d updateDiff) String() string {
	return fmt.Sprintf("updateDiff{newDependants: %v, newDependencies: %v}", d.newDependants, d.newDependencies)
}

// update the DAG (if needed) with the re-executed execution node
// returns the newly added dependencies/dependants for the new node
func (dag *dependencyDag) update(newNode *executionNode) updateDiff {
	oldNode := dag.nodes[newNode.seqId]

	if oldNode == nil {
		panic(fmt.Sprintf("cannot call update for new node with seqId %d", newNode.seqId))
	}

	beforeDependants := dag.dependants(newNode.seqId)
	beforeDependencies := dag.dependencies(newNode.seqId)

	// TODO(perf): Incrementally (without rebuilding the whole graph) compute edges?
	dag.nodes[newNode.seqId] = newNode
	dag.computeEdges()

	afterDependants := dag.dependants(newNode.seqId)
	afterDependencies := dag.dependencies(newNode.seqId)

	return updateDiff{
		newDependants:   subtract(afterDependants, beforeDependants),
		newDependencies: subtract(afterDependencies, beforeDependencies),
	}
}

func (dag *dependencyDag) execute(state *accountDelta, nWorkers int) {
	queue := newChannelProcessingQueue()
	executor := newDagExecutor(dag, queue)
	var mu sync.Mutex
	var iteration = 1
	var executionOrder []int

	for workerId := 0; workerId < nWorkers; workerId++ {
		go func(mu *sync.Mutex, workerId int, q <-chan processingTask) {
			for task := range q {
				// TODO: Stop locking for the whole execution scope, but instead lock per state/dag update
				mu.Lock()
				executionOrder = append(executionOrder, task.nodeSeqId)
				node := dag.lookup(task.nodeSeqId)

				// TODO(perf): Can we sometimes not execute the node again (e.g. if it's the first transaction)?
				// We don't know if a transaction was executed in the correct order,
				// so we must always re-execute it to see if anything changed, until we traverse the graph fully.
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
					state.RevertUpdates(descendantNode.seqId, descendantNode.updates)
					executor.markUnvisited(descendantSeqId)

					// TODO(perf): Only push the ones that weren't visited yet
					dq = append(dq, dag.dependants(descendantSeqId)...)
				}

				if len(diff.newDependencies) == 0 {
					state.ApplyUpdates(reExecutedNode.seqId, reExecutedNode.updates)
				} else {
					// This node was moved to a different part of the DAG
					// and should be processed again at a later point.
					executor.markUnvisited(reExecutedNode.seqId)
					for _, seqId := range diff.newDependencies {
						executor.markUnvisited(seqId)
					}
					state.RevertUpdates(node.seqId, node.updates)
				}

				fmt.Printf("# node: %s\n", node.String())
				fmt.Printf("# re-executed node: %s\n", reExecutedNode.String())
				fmt.Printf("# diff: %s\n", diff.String())
				fmt.Printf("# execution order: %v\n", executionOrder)
				fmt.Printf("# execution queue: %v\n", queue.String())
				fmt.Printf("# visited: %v\n", executor.visited)
				fmt.Printf("# state: %s\n", state.String())
				fmt.Println(dag.Graphviz(fmt.Sprintf("After_%d_iter_%d", reExecutedNode.seqId, iteration), fmt.Sprintf("%d", iteration), reExecutedNode.seqId))
				fmt.Println()

				iteration++
				mu.Unlock()
				task.done()
			}
		}(&mu, workerId, queue.tasks())
	}

	executor.execute()

	fmt.Printf("execution order: %v\n", executionOrder)
	fmt.Println(queue.String())
}

// Graphviz outputs a graph in DOT graphing language.
// See: https://graphviz.org/doc/info/lang.html
// View online: https://dreampuf.github.io/GraphvizOnline
func (dag *dependencyDag) Graphviz(graphName, nodePostfix string, highlightedSeqId int) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("subgraph %s {\n", graphName))

	nodeName := func(seqId int) string {
		return fmt.Sprintf("n_%d_%s", seqId, nodePostfix)
	}
	intToHex := func(num int) string {
		hex := fmt.Sprintf("%x", num)
		if len(hex) == 1 {
			hex = "0" + hex
		}
		return hex
	}
	randomHexColor := func() string {
		minColor := 180
		red := minColor + rand.Intn(255-minColor)
		green := minColor + rand.Intn(255-minColor)
		blue := minColor + rand.Intn(255-minColor)
		return "#" + intToHex(red) + intToHex(green) + intToHex(blue)
	}

	color := randomHexColor()
	sb.WriteString(fmt.Sprintf("\tnode [style=filled,color=\"%s\"];\n", color))
	for _, node := range dag.nodes {
		if node.seqId == highlightedSeqId {
			sb.WriteString(fmt.Sprintf("\t%s [color=\"#ffa500\", penwidth=3, fillcolor=\"%s\", style=\"filled\"];\n", nodeName(node.seqId), color))
		}
		if len(dag.dependants(node.seqId)) == 0 && len(dag.dependencies(node.seqId)) == 0 {
			sb.WriteString(fmt.Sprintf("\t%s;\n", nodeName(node.seqId)))
			continue
		}
		for _, depSeqId := range dag.dependants(node.seqId) {
			sb.WriteString(fmt.Sprintf("\t%s -> %s ", nodeName(node.seqId), nodeName(depSeqId)))
			sb.WriteString(fmt.Sprintf(
				"[label=\"%s\", fontsize=8, fontcolor=\"#a0a0a0\"];\n",
				conflictingAccounts(node, dag.lookup(depSeqId)),
			))
		}
	}
	sb.WriteString("}")
	return sb.String()
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
			out.WriteString(fmt.Sprintf("%d", e.nodeSeqId))
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

// conflictingAccounts returns account names that cause the dependency relationship between given nodes.
func conflictingAccounts(dependency, dependant *executionNode) []string {
	intersections := make(map[string]bool)
	for _, name := range intersect(dependency.updates, dependant.reads) {
		intersections[name] = true
	}
	for _, name := range intersect(dependant.updates, dependency.reads) {
		intersections[name] = true
	}
	var unique []string
	for name := range intersections {
		unique = append(unique, name)
	}
	return unique
}

func intersect(updates []api.AccountUpdate, reads []string) []string {
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
