package concurrent_transaction_executor

import "fmt"

type transfer struct {
	from  string
	to    string
	value uint
}

func (t transfer) Updates(state AccountState) ([]AccountUpdate, error) {
	fromAcc := state.GetAccount(t.from)
	if fromAcc.Balance < t.value {
		return nil, fmt.Errorf("insufficient balance")
	}
	return []AccountUpdate{
		{Name: t.from, BalanceChange: -t.value},
		{Name: t.to, BalanceChange: t.value},
	}, nil
}
