package parallel

import (
	"blockchain/executor/types"
	"blockchain/transactions"
	"testing"
)

func TestReExecutions_DependencyAddition_IndependentNodes(t *testing.T) {
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
			// This transaction will first be treated as independent,
			// but should be moved to the dependency subgraph
			// after the above transaction is re-executed.
			transactions.Transfer{From: "C", To: "D", Value: 20},
			// Next 3 transactions are independent
			transactions.Transfer{From: "E", To: "F", Value: 10},
			transactions.Transfer{From: "G", To: "H", Value: 10},
			transactions.Transfer{From: "I", To: "J", Value: 10},
		},
	}

	txExec, delta, _ := execute(t, block, startState)

	assertExecutionCountEqual(t, txExec, 0, 1)
	// Tx 1 has no inputs, so no other transaction can affect it's output.
	assertExecutionCountEqual(t, txExec, 1, 1)
	assertExecutionCountEqual(t, txExec, 2, 2)
	assertExecutionCountEqual(t, txExec, 3, 2)
	assertExecutionCountEqual(t, txExec, 4, 1)
	assertExecutionCountEqual(t, txExec, 5, 1)
	assertExecutionCountEqual(t, txExec, 6, 1)

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

	assertDelta(t, &expectedDelta, delta)
}

func TestReExecutions_IncrementallyDependantNodes(t *testing.T) {
	startState := testAccountState{
		types.AccountValue{Name: "A", Balance: 0},
		types.AccountValue{Name: "B", Balance: 0},
		types.AccountValue{Name: "C", Balance: 0},
	}

	block := types.Block{
		Transactions: []types.Transaction{
			transactions.Mint{To: "A", Value: 20},
			transactions.Transfer{From: "A", To: "B", Value: 10},
			transactions.Transfer{From: "A", To: "C", Value: 10},
		},
	}

	txExec, delta, _ := execute(t, block, startState)

	assertExecutionCountEqual(t, txExec, 0, 1)
	assertExecutionCountEqual(t, txExec, 1, 2)
	assertExecutionCountEqual(t, txExec, 2, 3)

	expectedDelta := accountDelta{
		oldState: startState,
		updatedBalances: map[string]int{
			"A": 0,
			"B": 10,
			"C": 10,
		},
	}

	assertDelta(t, &expectedDelta, delta)
}

func execute(
	t *testing.T,
	block types.Block,
	startState types.AccountState,
) (*transactionExecutor, *accountDelta, *dagExecutor) {
	nWorkers := 5
	txExec := newTransactionExecutor(len(block.Transactions))
	optimisticExec := newOptimisticExecutor(&txExec, block.Transactions, startState)
	nodes := optimisticExec.execute(nWorkers)

	for _, node := range nodes {
		assertExecutionCountEqual(t, &txExec, node.SeqId, 1)
	}

	dag := NewDependencyDag(nodes)
	queue := newChannelProcessingQueue()
	delta := newAccountDelta(startState)

	dagExec := newDagExecutor(dag, &queue, &txExec, delta)
	dagExec.execute(nWorkers)

	return &txExec, delta, &dagExec
}

func assertDelta(t *testing.T, expected, actual *accountDelta) {
	if actual.String() != expected.String() {
		t.Fatalf(`expected updated startState "%s" but got "%s"`, expected.String(), actual.String())
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
