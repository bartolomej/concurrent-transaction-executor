package transactions

import (
	"blockchain/executor"
	"fmt"
)

type Transfer struct {
	From  string
	To    string
	Value uint
}

func (t Transfer) Updates(state executor.AccountState) ([]executor.AccountUpdate, error) {
	fromAcc := state.GetAccount(t.From)
	if fromAcc.Balance < t.Value {
		return nil, fmt.Errorf("insufficient balance")
	}
	return []executor.AccountUpdate{
		{Name: t.From, BalanceChange: -t.Value},
		{Name: t.To, BalanceChange: t.Value},
	}, nil
}
