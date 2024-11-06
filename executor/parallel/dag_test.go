package parallel

import (
	"blockchain/executor/types"
	"slices"
	"testing"
)

func TestSimpleDagBuilding(t *testing.T) {
	nodes := []*ExecutionNode{
		{
			SeqId: 0,
			Reads: []string{"A", "B"},
			Updates: []types.AccountUpdate{
				{Name: "A"},
			},
		},
		{
			SeqId: 1,
			Reads: []string{"A"},
			Updates: []types.AccountUpdate{
				{Name: "B"},
			},
		},
		{
			SeqId: 2,
			Reads: []string{"A", "B"},
			Updates: []types.AccountUpdate{
				{Name: "A"},
				{Name: "B"},
			},
		},
	}

	actual := NewDependencyDag(nodes)

	expected := &DependencyDag{
		nodes: []*ExecutionNode{
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

func TestComplexDagBuilding(t *testing.T) {
	nodes := []*ExecutionNode{
		{
			SeqId: 0,
			Reads: []string{"A"},
			Updates: []types.AccountUpdate{
				{Name: "A", BalanceChange: -10},
				{Name: "B", BalanceChange: 10},
			},
		},
		{
			SeqId: 1,
			Reads: []string{"A"},
			Updates: []types.AccountUpdate{
				{Name: "A", BalanceChange: -10},
				{Name: "C", BalanceChange: 10},
			},
		},
		{
			SeqId:   2,
			Reads:   []string{"B"},
			Updates: []types.AccountUpdate{},
		},
		// If N+1 node Updates state that N node Reads,
		// N+1 node must be scheduled after N, since it can affect its output.
		{
			SeqId: 3,
			Reads: []string{"D"},
			Updates: []types.AccountUpdate{
				{Name: "B"},
			},
		},
	}

	actual := NewDependencyDag(nodes)

	expected := &DependencyDag{
		nodes: []*ExecutionNode{
			0: nodes[0],
			1: nodes[1],
			2: nodes[2],
			3: nodes[3],
		},
		dependantsById: map[int]map[int]bool{
			0: {
				1: true,
				2: true,
			},
			2: {
				3: true,
			},
		},
		dependenciesById: map[int]map[int]bool{
			1: {
				0: true,
			},
			2: {
				0: true,
			},
			3: {
				2: true,
			},
		},
	}

	assertDagEqual(t, actual, expected)

	testQueue := newTestDagQueue()
	executor := newDagExecutor(actual, testQueue)
	executor.traverse()

	expectedWalkOrder := [][]int{
		{0},
		{1, 2},
		{3},
	}

	assertQueueEqual(t, testQueue, expectedWalkOrder)
}

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
	executor := newDagExecutor(dag, testQueue)
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
	executor := newDagExecutor(dag, testQueue)
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

func assertDagEqual(t *testing.T, actual, expected *DependencyDag) {
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
