package transactions

import (
	"blockchain/executor/api"
	"fmt"
)

type Transfer struct {
	From  string
	To    string
	Value uint
}

func (t Transfer) Updates(state api.AccountState) ([]api.AccountUpdate, error) {
	fromAcc := state.GetAccount(t.From)
	if fromAcc.Balance < t.Value {
		return nil, fmt.Errorf("insufficient balance")
	}
	return []api.AccountUpdate{
		{Name: t.From, BalanceChange: -int(t.Value)},
		{Name: t.To, BalanceChange: int(t.Value)},
	}, nil
}
