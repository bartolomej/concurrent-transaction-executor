package parallel

import (
	"blockchain/executor"
	"slices"
	"testing"
)

func TestDag1(t *testing.T) {
	nodes := []*executionNode{
		{
			seqId: 0,
			reads: []string{"A", "B"},
			updates: []executor.AccountUpdate{
				{Name: "A"},
			},
		},
		{
			seqId: 1,
			reads: []string{"A"},
			updates: []executor.AccountUpdate{
				{Name: "B"},
			},
		},
		{
			seqId: 2,
			reads: []string{"A", "B"},
			updates: []executor.AccountUpdate{
				{Name: "A"},
				{Name: "B"},
			},
		},
	}

	actual := newDependencyDag(nodes)

	expected := &dependencyDag{
		nodes: map[int]*executionNode{
			0: nodes[0],
			1: nodes[1],
			2: nodes[2],
		},
		dependantsById: map[int]map[int]bool{
			0: {},
			1: {0: true},
			2: {0: true, 1: true},
		},
		dependenciesById: map[int]map[int]bool{
			0: {1: true, 2: true},
			1: {2: true},
			2: {},
		},
	}

	assertEqual(t, actual, expected)
}

type testDagQueue struct {
	batches [][]int
}

func newTestDagQueue() *testDagQueue {
	return &testDagQueue{
		batches: make([][]int, 0),
	}
}

func (q *testDagQueue) addBatch(batch []dagQueueElement) {
	idBatch := make([]int, 0)
	for _, element := range batch {
		idBatch = append(idBatch, element.nodeId)
		element.wg.Done()
	}
	q.batches = append(q.batches, idBatch)
}

func (q *testDagQueue) close() {
	// noop
}

func TestTopologicalOrder1(t *testing.T) {
	dag := newDependencyDag([]*executionNode{
		{
			seqId: 0,
			reads: []string{"A"},
			updates: []executor.AccountUpdate{
				{Name: "A"},
			},
		},
		{
			seqId: 1,
			reads: []string{"A"},
			updates: []executor.AccountUpdate{
				{Name: "B"},
			},
		},
		{
			seqId: 2,
			reads: []string{"A"},
		},
		{
			seqId: 3,
			reads: []string{"A", "B"},
		},
	})

	testQueue := newTestDagQueue()
	dag.concurrentWalk(testQueue)

	expected := [][]int{
		{0},
		{1, 2},
		{3},
	}
	actual := testQueue.batches

	for i := range expected {
		if !slices.Equal(actual[i], expected[i]) {
			t.Errorf("actual[%d] %v != expected[%d] %v", i, actual, i, expected)
		}
	}
}

func assertEqual(t *testing.T, actual, expected *dependencyDag) {
	if actual.String() != expected.String() {
		t.Errorf("actual %v, expected %v", actual, expected)
	}
}
