package executor

import (
	"blockchain/executor/api"
	"blockchain/executor/parallel"
	"blockchain/executor/serial"
	"blockchain/transactions"
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestExecutorTransferTransaction1(t *testing.T) {
	startState := testAccountState{
		api.AccountValue{Name: "A", Balance: 20},
		api.AccountValue{Name: "B", Balance: 30},
		api.AccountValue{Name: "C", Balance: 40},
	}
	block := api.Block{
		Transactions: []api.Transaction{
			transactions.Transfer{From: "A", To: "B", Value: 5},
			transactions.Transfer{From: "B", To: "C", Value: 10},
			transactions.Transfer{From: "B", To: "C", Value: 30}, // should fail
		},
	}
	expectedUpdateState := []api.AccountValue{
		{Name: "A", Balance: 15},
		{Name: "B", Balance: 25},
		{Name: "C", Balance: 50},
	}
	assertExecution(t, expectedUpdateState, block, startState, serial.NewExecutor(), "serial")
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(3), "parallel")
}

func TestExecutorTransferTransaction2(t *testing.T) {
	startState := testAccountState{
		api.AccountValue{Name: "A", Balance: 10},
		api.AccountValue{Name: "B", Balance: 20},
		api.AccountValue{Name: "C", Balance: 30},
		api.AccountValue{Name: "D", Balance: 40},
	}
	block := api.Block{
		Transactions: []api.Transaction{
			transactions.Transfer{From: "A", To: "B", Value: 5},
			transactions.Transfer{From: "C", To: "D", Value: 10},
		},
	}
	expectedUpdateState := []api.AccountValue{
		{Name: "A", Balance: 5},
		{Name: "B", Balance: 25},
		{Name: "C", Balance: 20},
		{Name: "D", Balance: 50},
	}
	assertExecution(t, expectedUpdateState, block, startState, serial.NewExecutor(), "serial")
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(2), "parallel")
}

func TestExecutorTransferTransaction3(t *testing.T) {
	startState := testAccountState{
		api.AccountValue{Name: "A", Balance: 20},
		api.AccountValue{Name: "B", Balance: 0},
		api.AccountValue{Name: "C", Balance: 0},
		api.AccountValue{Name: "D", Balance: 0},
		api.AccountValue{Name: "E", Balance: 10},
		api.AccountValue{Name: "F", Balance: 0},
		api.AccountValue{Name: "G", Balance: 0},
	}
	block := api.Block{
		Transactions: []api.Transaction{
			transactions.Transfer{From: "A", To: "B", Value: 10},
			transactions.Transfer{From: "A", To: "C", Value: 10},
			// At initial execution, this will produce no updates due to "insufficient balance" error
			transactions.Transfer{From: "B", To: "D", Value: 10},
			// After initial execution, this will be incorrectly executed before the above tx
			transactions.Transfer{From: "D", To: "E", Value: 10},
			// After initial execution, this will be incorrectly executed (will perform a transfer, when it shouldn't),
			conditionalBulkTransfer{
				from:               []string{"E"},
				to:                 []string{"F"},
				value:              10,
				untilBalanceEquals: 10,
			},
			transactions.Transfer{From: "F", To: "G", Value: 10},
		},
	}
	expectedUpdateState := []api.AccountValue{
		{Name: "A", Balance: 0},
		{Name: "B", Balance: 0},
		{Name: "C", Balance: 10},
		{Name: "D", Balance: 0},
		{Name: "E", Balance: 20},
	}
	assertExecution(t, expectedUpdateState, block, startState, serial.NewExecutor(), "serial")
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(3), "parallel")
}

func TestExecutorConditionalTransaction4(t *testing.T) {
	startState := testAccountState{
		api.AccountValue{Name: "A", Balance: 10},
		api.AccountValue{Name: "B", Balance: 10},
		api.AccountValue{Name: "C", Balance: 0},
		api.AccountValue{Name: "D", Balance: 0},
	}
	mintTx := transactions.Mint{To: "B", Value: 5}
	condBulkTransferTx := conditionalBulkTransfer{
		from:               []string{"A", "B"},
		to:                 []string{"C", "D"},
		value:              10,
		untilBalanceEquals: 10,
	}
	block1 := api.Block{
		Transactions: []api.Transaction{
			longRunningTx{
				duration:  time.Second,
				delayedTx: mintTx,
			},
			condBulkTransferTx,
		},
	}
	block2 := api.Block{
		Transactions: []api.Transaction{
			mintTx,
			longRunningTx{
				duration:  time.Second,
				delayedTx: condBulkTransferTx,
			},
		},
	}
	expectedUpdateState := []api.AccountValue{
		{Name: "A", Balance: 0},
		{Name: "B", Balance: 15},
		{Name: "C", Balance: 10},
	}

	assertExecution(t, expectedUpdateState, block1, startState, serial.NewExecutor(), "serial")
	assertExecution(t, expectedUpdateState, block2, startState, serial.NewExecutor(), "parallel")

	// TODO: Run multiple times to account for randomness
	assertExecution(t, expectedUpdateState, block1, startState, parallel.NewExecutor(3), "serial")
	assertExecution(t, expectedUpdateState, block2, startState, parallel.NewExecutor(3), "parallel")
}

func TestExecutorConditionalTransaction5(t *testing.T) {
	startState := testAccountState{
		api.AccountValue{Name: "A", Balance: 0},
	}
	block := api.Block{
		Transactions: []api.Transaction{
			transactions.Mint{To: "A", Value: 10},
			transactions.Transfer{From: "A", To: "B", Value: 10},
			conditionalBulkTransfer{
				from:               []string{"A", "B"},
				to:                 []string{"C", "D"},
				value:              10,
				untilBalanceEquals: 10,
			},
			transactions.Transfer{From: "C", To: "E", Value: 10},
			transactions.Transfer{From: "D", To: "E", Value: 10},
		},
	}
	expectedUpdateState := []api.AccountValue{
		{Name: "A", Balance: 0},
		{Name: "B", Balance: 10},
	}

	assertExecution(t, expectedUpdateState, block, startState, serial.NewExecutor(), "serial")
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(3), "parallel")
}

// TODO: Concurrent execution doesn't scale well for increasing values of maxDepth
// TODO: Fix concurrent issues for increasing values of maxDepth
func TestParallelExecutionWithIndependentBranches(t *testing.T) {
	rootAccount := "A"
	leafAccount := "B"
	maxDepth := 2
	rootBalance := uint(math.Pow(2, float64(maxDepth)))

	startState := testAccountState{
		api.AccountValue{Name: rootAccount, Balance: rootBalance},
	}

	var block = api.Block{
		Transactions: buildBinaryTxTree(0, rootAccount, rootBalance, leafAccount),
	}

	var expectedUpdateState = []api.AccountValue{
		{Name: rootAccount, Balance: 0},
		{Name: leafAccount, Balance: rootBalance},
	}

	// Note: A noticeable perf improvement for parallel execution is only visible
	// when transactions are doing more intensive work (e.g. loops with 1000+ iterations or I/O)
	assertExecution(t, expectedUpdateState, block, startState, serial.NewExecutor(), "serial")
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(10), "parallel")
}

// buildBinaryTxTree builds a binary tree that redistributes the values from root account to the end account
// where the transaction dependency graph is in the shape of a binary tree with height low2(rootBalance)
func buildBinaryTxTree(id int, from string, amount uint, endAccount string) []api.Transaction {
	var txs []api.Transaction
	var to = from + strconv.FormatInt(int64(id), 10)
	// Stop condition - subdivide the amount anymore
	var isLeaf = amount == 1

	if isLeaf {
		// Transfer to the same account at the end, so that it's easier to test
		to = endAccount
	}

	txs = append(txs, transactions.Transfer{
		From:  from,
		To:    to,
		Value: amount,
	})

	if isLeaf {
		return txs
	}

	txs = append(txs, buildBinaryTxTree(id+1, to, amount/2, endAccount)...)
	txs = append(txs, buildBinaryTxTree(id+2, to, amount/2, endAccount)...)

	return txs
}

type longRunningTx struct {
	duration  time.Duration
	delayedTx api.Transaction
}

func (tx longRunningTx) Updates(state api.AccountState) ([]api.AccountUpdate, error) {
	time.Sleep(tx.duration)
	return tx.delayedTx.Updates(state)
}

// conditionalBulkTransfer empties from balances and transfers them to
// until an account with untilBalanceEquals is found.
// Note: This is only used for testing purposes.
type conditionalBulkTransfer struct {
	from               []string
	to                 []string
	value              uint
	untilBalanceEquals uint
}

func (t conditionalBulkTransfer) Updates(state api.AccountState) ([]api.AccountUpdate, error) {
	var updates []api.AccountUpdate
	for i, from := range t.from {
		to := t.to[i]
		acc := state.GetAccount(from)
		if acc.Balance == t.untilBalanceEquals {
			transfer := transactions.Transfer{
				From:  from,
				To:    to,
				Value: t.value,
			}
			transferUpdates, transferErr := transfer.Updates(state)
			if transferErr != nil {
				return nil, transferErr
			}
			updates = append(updates, transferUpdates...)
		} else {
			break
		}
	}
	return updates, nil
}

type testAccountState []api.AccountValue

func (s testAccountState) GetAccount(name string) api.AccountValue {
	for _, v := range s {
		if v.Name == name {
			return v
		}
	}
	return api.AccountValue{Name: name, Balance: 0}
}

func assertExecution(
	t *testing.T,
	expectedUpdatedState []api.AccountValue,
	block api.Block,
	startState api.AccountState,
	executor api.BlockExecutor,
	label string,
) {
	start := time.Now()
	actualUpdatedState, err := executor.ExecuteBlock(block, startState)
	elapsed := time.Since(start)

	sort.Slice(actualUpdatedState, func(i, j int) bool {
		return actualUpdatedState[i].Name < actualUpdatedState[j].Name
	})
	sort.Slice(expectedUpdatedState, func(i, j int) bool {
		return expectedUpdatedState[i].Name < expectedUpdatedState[j].Name
	})

	fmt.Printf("Execution for %s took %s\n", label, elapsed)

	if err != nil {
		t.Errorf("unexpected error while executing block: %e", err)
	}

	if len(expectedUpdatedState) != len(actualUpdatedState) {
		t.Fatalf("expected %d updates, but got %d", len(expectedUpdatedState), len(actualUpdatedState))
	}

	for i, expected := range expectedUpdatedState {
		actual := actualUpdatedState[i]
		if expected.Name != actual.Name {
			t.Errorf("name assertion failed -> expected: %s, actual: %s", expected.Name, actual.Name)
		}
		if expected.Balance != actual.Balance {
			t.Errorf("balance assertion failed for %s -> expected: %d, actual: %d", expected.Name, expected.Balance, actual.Balance)
		}
	}
}
