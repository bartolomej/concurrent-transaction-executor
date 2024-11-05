package transactions

import (
	"blockchain/executor/types"
)

type Mint struct {
	To    string
	Value uint
}

func (t Mint) Updates(state types.AccountState) ([]types.AccountUpdate, error) {
	return []types.AccountUpdate{
		{Name: t.To, BalanceChange: int(t.Value)},
	}, nil
}
