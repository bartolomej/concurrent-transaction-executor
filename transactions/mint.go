package transactions

import (
	"blockchain/executor/api"
)

type Mint struct {
	To    string
	Value uint
}

func (t Mint) Updates(state api.AccountState) ([]api.AccountUpdate, error) {
	return []api.AccountUpdate{
		{Name: t.To, BalanceChange: int(t.Value)},
	}, nil
}
