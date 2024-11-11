package parallel

import (
	"blockchain/executor/types"
	"slices"
	"testing"
)

func TestSimpleScheduling(t *testing.T) {
	dag := NewDependencyDag([]*ExecutionNode{
		{
			SeqId: 0,
			Reads: []string{"A"},
			Updates: []types.AccountUpdate{
				{Name: "A"},
			},
		},
		{
			SeqId: 1,
			Reads: []string{"B"},
			Updates: []types.AccountUpdate{
				{Name: "B"},
			},
		},
		{
			SeqId: 2,
			Reads: []string{"A"},
			Updates: []types.AccountUpdate{
				{Name: "B"},
			},
		},
		{
			SeqId: 3,
			Reads: []string{"A"},
		},
		{
			SeqId: 4,
			Reads: []string{"A", "B"},
		},
	})

	testQueue := newTestDagQueue()
	executor := newDagExecutor(dag, testQueue, nil, nil)
	executor.traverse()

	expected := [][]int{
		{0, 1},
		{2, 3},
		{4},
	}

	assertQueueEqual(t, testQueue, expected)
}

// Verifies that long independent branches are processed concurrently
func TestLongIndependentBranchesScheduling(t *testing.T) {
	dag := NewDependencyDag([]*ExecutionNode{
		{
			SeqId: 0,
			Reads: []string{"A"},
			Updates: []types.AccountUpdate{
				{Name: "B"},
			},
		},
		{
			SeqId: 1,
			Reads: []string{"A"},
			Updates: []types.AccountUpdate{
				{Name: "C"},
			},
		},
		{
			SeqId: 2,
			Reads: []string{"B"},
			Updates: []types.AccountUpdate{
				{Name: "D"},
			},
		},
		{
			SeqId: 3,
			Reads: []string{"C"},
			Updates: []types.AccountUpdate{
				{Name: "E"},
			},
		},
		{
			SeqId: 4,
			Reads: []string{"D"},
		},
		{
			SeqId: 5,
			Reads: []string{"E"},
		},
	})

	testQueue := newTestDagQueue()
	executor := newDagExecutor(dag, testQueue, nil, nil)
	executor.traverse()

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

func (q *testDagQueue) enqueue(units []processingTask) {
	idBatch := make([]int, 0)
	for _, unit := range units {
		idBatch = append(idBatch, unit.seqId)
		unit.done()
	}
	q.batches = append(q.batches, idBatch)
}

func (q *testDagQueue) tasks() <-chan processingTask {
	// Not needed for testing
	panic("unimplemented")
}

func (q *testDagQueue) close() {
	// noop
}

var _ processingQueue = &testDagQueue{}

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
