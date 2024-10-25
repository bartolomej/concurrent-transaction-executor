package concurrent_transaction_executor

import "testing"

func TestExecutorTransferTransaction1(t *testing.T) {
	startState := testAccountState{
		AccountValue{Name: "A", Balance: 20},
		AccountValue{Name: "B", Balance: 30},
		AccountValue{Name: "C", Balance: 40},
	}
	block := Block{
		Transactions: []Transaction{
			transfer{from: "A", to: "B", value: 5},
			transfer{from: "B", to: "C", value: 10},
			transfer{from: "B", to: "C", value: 30}, // should fail
		},
	}
	expectedUpdateState := []AccountValue{
		{Name: "A", Balance: 15},
		{Name: "B", Balance: 25},
		{Name: "C", Balance: 50},
	}
	actualUpdateState, err := ExecuteBlock(block, startState)
	if err != nil {
		t.Error(err)
	}
	assertAccountValuesEqual(t, expectedUpdateState, actualUpdateState)
}

type testAccountState []AccountValue

func (s testAccountState) GetAccount(name string) AccountValue {
	for _, v := range s {
		if v.Name == name {
			return v
		}
	}
	return AccountValue{Name: name, Balance: 0}
}

func assertAccountValuesEqual(t *testing.T, expectedState []AccountValue, actualState []AccountValue) {
	for i, v := range expectedState {
		if v.Name != actualState[i].Name {
			t.Errorf("name assertion failed -> expected: %s, actual: %s", v.Name, actualState[i].Name)
		}
		if v.Balance != actualState[i].Balance {
			t.Errorf("balance assertion failed -> expected: %s, actual: %s", v.Name, actualState[i].Name)
		}
	}
}
