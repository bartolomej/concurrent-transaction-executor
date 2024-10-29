package parallel

import (
	"blockchain/executor"
	"slices"
	"testing"
)

func TestDag1(t *testing.T) {
	orderedReports := []executionReport{
		{
			index: 0,
			reads: []string{"A", "B"},
			updates: []executor.AccountUpdate{
				{Name: "A"},
			},
		},
		{
			index: 1,
			reads: []string{"A"},
			updates: []executor.AccountUpdate{
				{Name: "B"},
			},
		},
		{
			index: 2,
			reads: []string{"A", "B"},
			updates: []executor.AccountUpdate{
				{Name: "A"},
				{Name: "B"},
			},
		},
	}

	actual := buildDependencyDag(orderedReports)

	expected := newDependencyDag([]*dependencyNode{
		{
			Id:           0,
			Dependencies: []int{},
		},
		{
			Id:           1,
			Dependencies: []int{0},
		},
		{
			Id:           2,
			Dependencies: []int{0, 1},
		},
	})

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
	dag := newDependencyDag([]*dependencyNode{
		{
			Id:           0,
			Dependencies: []int{},
		},
		{
			Id:           1,
			Dependencies: []int{0},
		},
		{
			Id:           2,
			Dependencies: []int{0},
		},
		{
			Id:           3,
			Dependencies: []int{0, 1},
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
