package transactions

import (
	"blockchain/executor/types"
	"fmt"
)

type Transfer struct {
	From  string
	To    string
	Value uint
}

func (t Transfer) Updates(state types.AccountState) ([]types.AccountUpdate, error) {
	fromAcc := state.Get(t.From)
	if fromAcc.Balance < t.Value {
		return nil, fmt.Errorf("insufficient balance")
	}
	return []types.AccountUpdate{
		{Name: t.From, BalanceChange: -int(t.Value)},
		{Name: t.To, BalanceChange: int(t.Value)},
	}, nil
}
