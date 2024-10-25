package concurrent_transaction_executor

type Block struct {
	Transactions []Transaction
}

type Transaction interface {
	Updates(AccountState) ([]AccountUpdate, error)
}

type AccountUpdate struct {
	Name          string
	BalanceChange uint
}

type AccountValue struct {
	Name    string
	Balance uint
}

// AccountState if the account does not exist, return zero balance
type AccountState interface {
	GetAccount(name string) AccountValue
}

// ExecuteBlock takes a Block with transactions, and returns
// the updated account and with the updated balance.
func ExecuteBlock(block Block, state AccountState) ([]AccountValue, error) {
	panic("implement me")
}
