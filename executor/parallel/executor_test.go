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
			Dependencies: []int{0, 1},
		},
		{
			Id:           3,
			Dependencies: []int{2},
		},
	})

	actual := dag.topologicalOrder()
	expected := []int{0, 1, 2, 3}
	if !slices.Equal(actual, expected) {
		t.Errorf("actual %v != expected %v", actual, expected)
	}
}

func assertEqual(t *testing.T, actual, expected *dependencyDag) {
	if actual.String() != expected.String() {
		t.Errorf("actual %v, expected %v", actual, expected)
	}
}
