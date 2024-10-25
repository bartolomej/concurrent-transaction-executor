package executor

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

type AccountState interface {
	// GetAccount returns zero balance if the account does not exist
	GetAccount(name string) AccountValue
}

type BlockExecutor interface {
	// ExecuteBlock takes a Block with transactions, and returns
	// the updated account and with the updated balance.
	ExecuteBlock(Block, AccountState) ([]AccountValue, error)
}
