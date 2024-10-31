package parallel

import (
	"blockchain/executor"
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
}

// buildDependencyDag produces a dependency DAG (Directed Acyclic Graph)
// given an ordered (by index) list of execution reports
func newDependencyDag(nodes []*executionNode) *dependencyDag {
	slices.SortFunc(nodes, func(a, b *executionNode) int {
		return a.seqId - b.seqId
	})

	seqIdsByUpdate := make(map[string]map[int]bool)
	dag := &dependencyDag{
		nodes:            make(map[int]*executionNode),
		dependantsById:   make(map[int]map[int]bool),
		dependenciesById: make(map[int]map[int]bool),
	}

	for _, node := range nodes {
		depSeqIds := make(map[int]bool)
		for _, read := range node.reads {
			for seqId, _ := range seqIdsByUpdate[read] {
				depSeqIds[seqId] = true
			}
		}
		dag.dependenciesById[node.seqId] = depSeqIds

		for depSeqId, _ := range depSeqIds {
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

	return dag
}

func (dag *dependencyDag) lookup(id int) *executionNode {
	return dag.nodes[id]
}

func (dag *dependencyDag) dependants(id int) []int {
	var seqIds []int

	for id, _ := range dag.dependantsById[id] {
		seqIds = append(seqIds, id)
	}

	return seqIds
}

func (dag *dependencyDag) dependencies(id int) []int {
	var seqIds []int

	for id, _ := range dag.dependenciesById[id] {
		seqIds = append(seqIds, id)
	}

	return seqIds
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
		inDegreeById[id] = len(dag.dependencies(node.seqId))
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
	sortedNodes := make([]*executionNode, 0, len(dag.nodes))
	for _, node := range dag.nodes {
		sortedNodes = append(sortedNodes, node)
	}
	slices.SortFunc(sortedNodes, func(a, b *executionNode) int {
		return a.seqId - b.seqId
	})
	for _, node := range sortedNodes {
		sb.WriteString(node.String())
		sb.WriteString("\n")
	}
	return sb.String()
}

type executionNode struct {
	// seqId is a unique identifier that also indicates the transaction order within a block
	seqId int
	// reads account names that were read by the transaction
	reads []string
	// updates account state changes that were produced by the transaction
	updates []executor.AccountUpdate
	// no updates were produced if err is set
	err error
	// transaction is the state transition function that produces reads, updates, err
	transaction executor.Transaction
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
