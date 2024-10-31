package parallel

import (
	"blockchain/executor/api"
	"slices"
	"testing"
)

func TestDag1(t *testing.T) {
	nodes := []*executionNode{
		{
			seqId: 0,
			reads: []string{"A", "B"},
			updates: []api.AccountUpdate{
				{Name: "A"},
			},
		},
		{
			seqId: 1,
			reads: []string{"A"},
			updates: []api.AccountUpdate{
				{Name: "B"},
			},
		},
		{
			seqId: 2,
			reads: []string{"A", "B"},
			updates: []api.AccountUpdate{
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
			0: {1: true, 2: true},
			1: {2: true},
			2: {},
		},
		dependenciesById: map[int]map[int]bool{
			0: {},
			1: {0: true},
			2: {0: true, 1: true},
		},
	}

	assertDagEqual(t, actual, expected)
}

func TestDagNodeRemoval(t *testing.T) {
	nodes := []*executionNode{
		{
			seqId: 0,
			reads: []string{"A", "B"},
			updates: []api.AccountUpdate{
				{Name: "A"},
			},
		},
		{
			seqId: 1,
			reads: []string{"A"},
			updates: []api.AccountUpdate{
				{Name: "B"},
			},
		},
		{
			seqId: 2,
			reads: []string{"A", "B"},
			updates: []api.AccountUpdate{
				{Name: "A"},
				{Name: "B"},
			},
		},
	}

	actual := newDependencyDag(nodes)
	actual.remove(1)

	expected := &dependencyDag{
		nodes: map[int]*executionNode{
			0: nodes[0],
			2: nodes[2],
		},
		dependantsById: map[int]map[int]bool{
			0: {2: true},
			2: {},
		},
		dependenciesById: map[int]map[int]bool{
			0: {},
			2: {0: true},
		},
	}

	assertDagEqual(t, actual, expected)
}

func TestConcurrentWalk1(t *testing.T) {
	dag := newDependencyDag([]*executionNode{
		{
			seqId: 0,
			reads: []string{"A"},
			updates: []api.AccountUpdate{
				{Name: "A"},
			},
		},
		{
			seqId: 1,
			reads: []string{"B"},
			updates: []api.AccountUpdate{
				{Name: "B"},
			},
		},
		{
			seqId: 2,
			reads: []string{"A"},
			updates: []api.AccountUpdate{
				{Name: "B"},
			},
		},
		{
			seqId: 3,
			reads: []string{"A"},
		},
		{
			seqId: 4,
			reads: []string{"A", "B"},
		},
	})

	testQueue := newTestDagQueue()
	dag.concurrentWalk(testQueue)

	expected := [][]int{
		{0, 1},
		{2, 3},
		{4},
	}

	assertQueueEqual(t, testQueue, expected)
}

// Verifies that long independent branches are visited concurrently
func TestConcurrentWalk2(t *testing.T) {
	dag := newDependencyDag([]*executionNode{
		{
			seqId: 0,
			reads: []string{"A"},
			updates: []api.AccountUpdate{
				{Name: "B"},
			},
		},
		{
			seqId: 1,
			reads: []string{"A"},
			updates: []api.AccountUpdate{
				{Name: "C"},
			},
		},
		{
			seqId: 2,
			reads: []string{"B"},
			updates: []api.AccountUpdate{
				{Name: "D"},
			},
		},
		{
			seqId: 3,
			reads: []string{"C"},
			updates: []api.AccountUpdate{
				{Name: "E"},
			},
		},
		{
			seqId: 4,
			reads: []string{"D"},
		},
		{
			seqId: 5,
			reads: []string{"E"},
		},
	})

	testQueue := newTestDagQueue()
	dag.concurrentWalk(testQueue)

	expected := [][]int{
		{0, 1},
		{2, 3},
		{4, 5},
	}

	assertQueueEqual(t, testQueue, expected)
}

type testDagQueue struct {
	batches [][]int
}

func newTestDagQueue() *testDagQueue {
	return &testDagQueue{
		batches: make([][]int, 0),
	}
}

func (q *testDagQueue) process(units []processingUnit) {
	idBatch := make([]int, 0)
	for _, unit := range units {
		idBatch = append(idBatch, unit.seqId)
		unit.done()
	}
	q.batches = append(q.batches, idBatch)
}

func (q *testDagQueue) close() {
	// noop
}

var _ processingQueue = &testDagQueue{}

func assertDagEqual(t *testing.T, actual, expected *dependencyDag) {
	if actual.String() != expected.String() {
		t.Errorf("actual %v, expected %v", actual, expected)
	}
}

func assertQueueEqual(t *testing.T, actualQueue *testDagQueue, expected [][]int) {
	actual := actualQueue.batches

	if len(actual) != len(expected) {
		t.Fatalf("wrong number of elements: expected %d, got %d", len(expected), len(actual))
	}

	for i := range expected {
		slices.Sort(actual[i])
		slices.Sort(expected[i])
		if !slices.Equal(actual[i], expected[i]) {
			t.Errorf("actual[%d] %v != expected[%d] %v", i, actual[i], i, expected[i])
		}
	}
}
