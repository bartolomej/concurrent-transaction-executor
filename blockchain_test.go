package blockchain

import (
	"blockchain/executor"
	"blockchain/executor/parallel"
	"blockchain/transactions"
	"testing"
)

func TestExecutorTransferTransaction1(t *testing.T) {
	startState := testAccountState{
		executor.AccountValue{Name: "A", Balance: 20},
		executor.AccountValue{Name: "B", Balance: 30},
		executor.AccountValue{Name: "C", Balance: 40},
	}
	block := executor.Block{
		Transactions: []executor.Transaction{
			transactions.Transfer{From: "A", To: "B", Value: 5},
			transactions.Transfer{From: "B", To: "C", Value: 10},
			transactions.Transfer{From: "B", To: "C", Value: 30}, // should fail
		},
	}
	expectedUpdateState := []executor.AccountValue{
		{Name: "A", Balance: 15},
		{Name: "B", Balance: 25},
		{Name: "C", Balance: 50},
	}
	//assertExecution(t, expectedUpdateState, block, startState, executor.NewExecutor())
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(3))
}

func TestExecutorTransferTransaction2(t *testing.T) {
	startState := testAccountState{
		executor.AccountValue{Name: "A", Balance: 10},
		executor.AccountValue{Name: "B", Balance: 20},
		executor.AccountValue{Name: "C", Balance: 30},
		executor.AccountValue{Name: "D", Balance: 40},
	}
	block := executor.Block{
		Transactions: []executor.Transaction{
			transactions.Transfer{From: "A", To: "B", Value: 5},
			transactions.Transfer{From: "C", To: "D", Value: 10},
		},
	}
	expectedUpdateState := []executor.AccountValue{
		{Name: "A", Balance: 5},
		{Name: "B", Balance: 25},
		{Name: "C", Balance: 20},
		{Name: "D", Balance: 50},
	}
	//assertExecution(t, expectedUpdateState, block, startState, serial.NewExecutor())
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(2))
}

func TestExecutorTransferTransaction3(t *testing.T) {
	startState := testAccountState{
		executor.AccountValue{Name: "A", Balance: 20},
		executor.AccountValue{Name: "B", Balance: 0},
		executor.AccountValue{Name: "C", Balance: 0},
	}
	block := executor.Block{
		Transactions: []executor.Transaction{
			transactions.Transfer{From: "A", To: "B", Value: 10},
			transactions.Transfer{From: "A", To: "C", Value: 10},
			transactions.Transfer{From: "B", To: "C", Value: 10},
		},
	}
	expectedUpdateState := []executor.AccountValue{
		{Name: "A", Balance: 0},
		{Name: "B", Balance: 0},
		{Name: "C", Balance: 20},
	}
	//assertExecution(t, expectedUpdateState, block, startState, serial.NewExecutor())
	assertExecution(t, expectedUpdateState, block, startState, parallel.NewExecutor(3))

}

type testAccountState []executor.AccountValue

func (s testAccountState) GetAccount(name string) executor.AccountValue {
	for _, v := range s {
		if v.Name == name {
			return v
		}
	}
	return executor.AccountValue{Name: name, Balance: 0}
}

func assertExecution(
	t *testing.T,
	expectedStateUpdate []executor.AccountValue,
	block executor.Block,
	startState executor.AccountState,
	executor executor.BlockExecutor,
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
			t.Errorf("balance assertion failed -> expected: %d, actual: %d", expected.Balance, actual.Balance)
		}
	}
}
