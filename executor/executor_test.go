package executor

import (
	"blockchain/executor/api"
	"blockchain/executor/parallel"
	"blockchain/executor/serial"
	"blockchain/transactions"
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
	//assertExecution(t, expectedUpdateState, block, startState, executor.NewExecutor())
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(3))
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
	assertExecution(t, expectedUpdateState, block, startState, serial.NewExecutor())
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(2))
}

func TestExecutorTransferTransaction3(t *testing.T) {
	startState := testAccountState{
		api.AccountValue{Name: "A", Balance: 20},
		api.AccountValue{Name: "B", Balance: 0},
		api.AccountValue{Name: "C", Balance: 0},
	}
	block := api.Block{
		Transactions: []api.Transaction{
			transactions.Transfer{From: "A", To: "B", Value: 10},
			transactions.Transfer{From: "A", To: "C", Value: 10},
			transactions.Transfer{From: "B", To: "C", Value: 10},
		},
	}
	expectedUpdateState := []api.AccountValue{
		{Name: "A", Balance: 0},
		{Name: "B", Balance: 0},
		{Name: "C", Balance: 20},
	}
	assertExecution(t, expectedUpdateState, block, startState, serial.NewExecutor())
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(3))
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
			newLongRunningTx(mintTx, time.Second),
			condBulkTransferTx,
		},
	}
	block2 := api.Block{
		Transactions: []api.Transaction{
			mintTx,
			newLongRunningTx(condBulkTransferTx, time.Second),
		},
	}
	expectedUpdateState := []api.AccountValue{
		{Name: "A", Balance: 0},
		{Name: "B", Balance: 15},
		{Name: "C", Balance: 10},
	}

	assertExecution(t, expectedUpdateState, block1, startState, serial.NewExecutor())
	assertExecution(t, expectedUpdateState, block2, startState, serial.NewExecutor())

	// TODO: Run multiple times to account for randomness
	assertExecution(t, expectedUpdateState, block1, startState, parallel.NewExecutor(3))
	assertExecution(t, expectedUpdateState, block2, startState, parallel.NewExecutor(3))
}

type longRunningTx struct {
	duration  time.Duration
	delayedTx api.Transaction
}

func newLongRunningTx(delayedTx api.Transaction, duration time.Duration) longRunningTx {
	return longRunningTx{
		delayedTx: delayedTx,
		duration:  duration,
	}
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
	expectedStateUpdate []api.AccountValue,
	block api.Block,
	startState api.AccountState,
	executor api.BlockExecutor,
) {
	actualStateUpdate, err := executor.ExecuteBlock(block, startState)
	if err != nil {
		t.Error(err)
	}
	for i, expected := range expectedStateUpdate {
		actual := actualStateUpdate[i]
		if expected.Name != actual.Name {
			t.Errorf("name assertion failed -> expected: %s, actual: %s", expected.Name, actual.Name)
		}
		if expected.Balance != actual.Balance {
			t.Errorf("balance assertion failed for %s -> expected: %d, actual: %d", expected.Name, expected.Balance, actual.Balance)
		}
	}
}
