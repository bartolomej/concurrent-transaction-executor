package executor

type SerialExecutor struct{}

func NewSerialExecutor() *SerialExecutor {
	return &SerialExecutor{}
}

func (e *SerialExecutor) ExecuteBlock(block Block, state AccountState) ([]AccountValue, error) {
	panic("not implemented")
}
