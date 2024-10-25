package executor

type ParallelExecutor struct{}

func NewParallelExecutor() *ParallelExecutor {
	return &ParallelExecutor{}
}

func (e *ParallelExecutor) ExecuteBlock(block Block, state AccountState) ([]AccountValue, error) {
	panic("not implemented")
}
