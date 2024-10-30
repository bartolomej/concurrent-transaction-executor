package transactions

import (
	"blockchain/executor"
)

type Mint struct {
	To    string
	Value uint
}

func (t Mint) Updates(state executor.AccountState) ([]executor.AccountUpdate, error) {
	return []executor.AccountUpdate{
		{Name: t.To, BalanceChange: int(t.Value)},
	}, nil
}
