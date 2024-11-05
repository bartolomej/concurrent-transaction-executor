package types

type Block struct {
	Transactions []Transaction
}

type Transaction interface {
	// Updates computes account state changes given the current state or returns an error.
	Updates(AccountState) ([]AccountUpdate, error)
}

type AccountUpdate struct {
	Name          string
	BalanceChange int
}

type AccountValue struct {
	Name    string
	Balance uint
}

type AccountState interface {
	// Get returns zero balance if the account does not exist
	Get(name string) AccountValue
}

type BlockExecutor interface {
	// Execute takes a Block with transactions, and returns
	// the updated account and with the updated balance.
	Execute(Block, AccountState) ([]AccountValue, error)
}
