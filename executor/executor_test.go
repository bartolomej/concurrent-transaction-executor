package executor

import (
	"blockchain/executor/parallel"
	"blockchain/executor/serial"
	"blockchain/executor/types"
	"blockchain/transactions"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"
)

// This test case is from the instructions doc (Example 1)
func TestSimpleSequentialTransactions(t *testing.T) {
	startState := testAccountState{
		types.AccountValue{Name: "A", Balance: 20},
		types.AccountValue{Name: "B", Balance: 30},
		types.AccountValue{Name: "C", Balance: 40},
	}
	block := types.Block{
		Transactions: []types.Transaction{
			transactions.Transfer{From: "A", To: "B", Value: 5},
			transactions.Transfer{From: "B", To: "C", Value: 10},
			transactions.Transfer{From: "B", To: "C", Value: 30}, // should fail
		},
	}
	expectedUpdateState := []types.AccountValue{
		{Name: "A", Balance: 15},
		{Name: "B", Balance: 25},
		{Name: "C", Balance: 50},
	}
	assertExecution(t, expectedUpdateState, block, startState, serial.NewExecutor(), "serial")
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(3), "parallel")
}

// This test case is from the instructions doc (Example 2)
func TestSimpleConcurrentTransactions(t *testing.T) {
	startState := testAccountState{
		types.AccountValue{Name: "A", Balance: 10},
		types.AccountValue{Name: "B", Balance: 20},
		types.AccountValue{Name: "C", Balance: 30},
		types.AccountValue{Name: "D", Balance: 40},
	}
	block := types.Block{
		Transactions: []types.Transaction{
			transactions.Transfer{From: "A", To: "B", Value: 5},
			transactions.Transfer{From: "C", To: "D", Value: 10},
		},
	}
	expectedUpdateState := []types.AccountValue{
		{Name: "A", Balance: 5},
		{Name: "B", Balance: 25},
		{Name: "C", Balance: 20},
		{Name: "D", Balance: 50},
	}
	assertExecution(t, expectedUpdateState, block, startState, serial.NewExecutor(), "serial")
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(2), "parallel")
}

func TestComplexConcurrentTransactions(t *testing.T) {
	startState := testAccountState{
		types.AccountValue{Name: "A", Balance: 20},
		types.AccountValue{Name: "B", Balance: 0},
		types.AccountValue{Name: "C", Balance: 0},
		types.AccountValue{Name: "D", Balance: 0},
		types.AccountValue{Name: "E", Balance: 10},
		types.AccountValue{Name: "F", Balance: 0},
		types.AccountValue{Name: "G", Balance: 0},
	}
	block := types.Block{
		Transactions: []types.Transaction{
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
	expectedUpdateState := []types.AccountValue{
		{Name: "A", Balance: 0},
		{Name: "C", Balance: 10},
		{Name: "E", Balance: 20},
	}
	assertExecution(t, expectedUpdateState, block, startState, serial.NewExecutor(), "serial")
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(3), "parallel")
}

func TestBlockingSequentialTransactions(t *testing.T) {
	startState := testAccountState{
		types.AccountValue{Name: "A", Balance: 10},
		types.AccountValue{Name: "B", Balance: 10},
		types.AccountValue{Name: "C", Balance: 0},
		types.AccountValue{Name: "D", Balance: 0},
	}
	mintTx := transactions.Mint{To: "B", Value: 5}
	condBulkTransferTx := conditionalBulkTransfer{
		from:               []string{"A", "B"},
		to:                 []string{"C", "D"},
		value:              10,
		untilBalanceEquals: 10,
	}
	block1 := types.Block{
		Transactions: []types.Transaction{
			sleepingTransaction{
				duration: time.Second,
				child:    mintTx,
			},
			condBulkTransferTx,
		},
	}
	block2 := types.Block{
		Transactions: []types.Transaction{
			mintTx,
			sleepingTransaction{
				duration: time.Second,
				child:    condBulkTransferTx,
			},
		},
	}
	expectedUpdateState := []types.AccountValue{
		{Name: "A", Balance: 0},
		{Name: "B", Balance: 15},
		{Name: "C", Balance: 10},
	}

	assertExecution(t, expectedUpdateState, block1, startState, serial.NewExecutor(), "serial")
	assertExecution(t, expectedUpdateState, block2, startState, serial.NewExecutor(), "serial")

	assertExecution(t, expectedUpdateState, block1, startState, parallel.NewExecutor(3), "parallel")
	assertExecution(t, expectedUpdateState, block2, startState, parallel.NewExecutor(3), "parallel")
}

func TestTreeLikeConcurrentTransactions(t *testing.T) {
	rootAccount := "A"
	leafAccount := "B"
	maxDepth := 4
	rootBalance := uint(math.Pow(2, float64(maxDepth)))

	startState := testAccountState{
		types.AccountValue{Name: rootAccount, Balance: rootBalance},
	}

	var id = 0
	var block = types.Block{}

	txs := buildTransferTree(&id, rootAccount, rootBalance, leafAccount)

	for _, tx := range txs {
		block.Transactions = append(block.Transactions, tx)
	}

	var expectedUpdateState = []types.AccountValue{
		{Name: rootAccount, Balance: 0},
		{Name: leafAccount, Balance: rootBalance},
	}

	// Note: A noticeable perf improvement for parallel execution is only visible
	// when transactions are doing more intensive work (e.g. loops with 1000+ iterations or I/O)
	assertExecution(t, expectedUpdateState, block, startState, serial.NewExecutor(), "serial")
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(10), "parallel")
}

// buildTransferTree builds a binary tree that redistributes the values from root account to the end account
// where the transaction dependency graph is in the shape of a binary tree with height low2(rootBalance)
func buildTransferTree(id *int, from string, amount uint, endAccount string) []sleepingTransaction {
	var txs []sleepingTransaction
	var to = fmt.Sprintf("%s_%d", from, *id)
	// Stop condition - subdivide the amount anymore
	var isLeaf = amount == 1

	if isLeaf {
		// Transfer to the same account at the end, so that it's easier to test
		to = endAccount
	}

	txs = append(txs, sleepingTransaction{
		duration: time.Millisecond * 100,
		child: transactions.Transfer{
			From:  from,
			To:    to,
			Value: amount,
		},
	})

	if isLeaf {
		return txs
	}

	*id++
	txs = append(txs, buildTransferTree(id, to, amount/2, endAccount)...)
	*id++
	txs = append(txs, buildTransferTree(id, to, amount/2, endAccount)...)

	return txs
}

// sleepingTransaction sleeps for duration before executing the child transaction to simulate I/O operations.
type sleepingTransaction struct {
	duration time.Duration
	child    types.Transaction
}

var _ types.Transaction = sleepingTransaction{}

func (tx sleepingTransaction) Updates(state types.AccountState) ([]types.AccountUpdate, error) {
	time.Sleep(tx.duration)
	return tx.child.Updates(state)
}

func (tx sleepingTransaction) String() string {
	return "sleepingTransaction"
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

var _ types.Transaction = conditionalBulkTransfer{}

func (tx conditionalBulkTransfer) Updates(state types.AccountState) ([]types.AccountUpdate, error) {
	var updates []types.AccountUpdate
	for i, from := range tx.from {
		to := tx.to[i]
		acc := state.Get(from)
		if acc.Balance == tx.untilBalanceEquals {
			transfer := transactions.Transfer{
				From:  from,
				To:    to,
				Value: tx.value,
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

func (tx conditionalBulkTransfer) String() string {
	return "conditionalBulkTransfer"
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

func assertExecution(
	t *testing.T,
	expectedUpdatedState []types.AccountValue,
	block types.Block,
	startState types.AccountState,
	executor types.BlockExecutor,
	label string,
) {
	start := time.Now()
	actualUpdatedState, err := executor.Execute(block, startState)
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

		t.Fatalf(
			"expected %d updates, but got %d:\n%s",
			len(expectedUpdatedState),
			len(actualUpdatedState),
			formatValues(actualUpdatedState),
		)
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

func formatValues(values []types.AccountValue) string {
	var serUpdates []string
	for _, v := range values {
		serUpdates = append(serUpdates, fmt.Sprintf("[%s, %d]", v.Name, v.Balance))
	}
	return strings.Join(serUpdates, ", ")
}
