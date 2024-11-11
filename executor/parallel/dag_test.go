package parallel

import (
	"blockchain/executor/types"
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
		// If N+1 node Updates startState that N node Reads,
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
}

func assertDagEqual(t *testing.T, actual, expected *DependencyDag) {
	if actual.String() != expected.String() {
		t.Errorf("actual %v, expected %v", actual, expected)
	}
}
