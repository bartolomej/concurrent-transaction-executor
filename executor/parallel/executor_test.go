package parallel

import (
	"blockchain/executor/types"
	"blockchain/transactions"
	"fmt"
	"testing"
)

func TestRealWorldIndependentTransactions(t *testing.T) {
	startState := testAccountState{
		types.AccountValue{Name: "A", Balance: 10},
		types.AccountValue{Name: "E", Balance: 10},
		types.AccountValue{Name: "G", Balance: 10},
		types.AccountValue{Name: "I", Balance: 10},
	}
	block := types.Block{
		Transactions: []types.Transaction{
			// First 3 transactions are inter-dependant
			transactions.Transfer{From: "A", To: "B", Value: 10},
			transactions.Mint{To: "A", Value: 20},
			transactions.Transfer{From: "A", To: "C", Value: 20},
			// This will first be treated as independent,
			// but will be correctly moved to the dependency subgraph
			// after the above transaction is re-executed.
			transactions.Transfer{From: "C", To: "D", Value: 20},
			// Next 3 transactions are independent
			transactions.Transfer{From: "E", To: "F", Value: 10},
			transactions.Transfer{From: "G", To: "H", Value: 10},
			transactions.Transfer{From: "I", To: "J", Value: 10},
		},
	}

	executor := NewExecutor(5)
	txExecutor := newTransactionExecutor(len(block.Transactions))
	nodes := executor.executeOptimistically(&txExecutor, block.Transactions, startState)

	for _, node := range nodes {
		assertExecutionCountEqual(t, &txExecutor, node.SeqId, 1)
	}

	dag := NewDependencyDag(nodes)

	fmt.Println(dag.Graphviz())

	delta := newAccountDelta(startState)
	dag.Execute(&txExecutor, delta, executor.nWorkers)
	fmt.Println(dag.Graphviz())

	assertExecutionCountEqual(t, &txExecutor, 0, 1)
	// Tx 1 has no inputs, so no other transaction can affect it's output.
	assertExecutionCountEqual(t, &txExecutor, 1, 1)
	assertExecutionCountEqual(t, &txExecutor, 2, 2)
	assertExecutionCountEqual(t, &txExecutor, 3, 2)
	assertExecutionCountEqual(t, &txExecutor, 4, 1)
	assertExecutionCountEqual(t, &txExecutor, 5, 1)
	assertExecutionCountEqual(t, &txExecutor, 6, 1)

	expectedDelta := accountDelta{
		oldState: startState,
		updatedBalances: map[string]int{
			"A": 0,
			"B": 10,
			"D": 20,
			"E": 0,
			"G": 0,
			"I": 0,
			"F": 10,
			"H": 10,
			"J": 10,
		},
	}

	if delta.String() != expectedDelta.String() {
		t.Fatalf(`expected updated state "%s" but got "%s"`, expectedDelta.String(), delta.String())
	}
}

func assertExecutionCountEqual(t *testing.T, txExecutor *transactionExecutor, seqId, expected int) {
	if txExecutor.executionCount[seqId] != expected {
		t.Errorf(
			"Expected execution count of %d, got %d for seqId=%d",
			expected,
			txExecutor.executionCount[seqId],
			seqId,
		)
	}
}

type testAccountState []types.AccountValue

func (s testAccountState) Get(name string) types.AccountValue {
	for _, v := range s {
		if v.Name == name {
			return v
		}
	}
	return types.AccountValue{Name: name, Balance: 0}
}
