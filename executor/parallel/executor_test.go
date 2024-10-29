package parallel

import (
	"blockchain/executor"
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

	expected := &dependencyDag{nodes: map[int]*dependencyNode{
		0: {
			Id:           0,
			Dependencies: []int{},
		},
		1: {
			Id:           1,
			Dependencies: []int{0},
		},
		2: {
			Id:           2,
			Dependencies: []int{0, 1},
		},
	}}

	assertEqual(t, actual, expected)
}

func assertEqual(t *testing.T, actual, expected *dependencyDag) {
	if actual.String() != expected.String() {
		t.Errorf("actual %v, expected %v", actual, expected)
	}
}
