package parallel

import (
	"blockchain/executor/types"
)

type Executor struct {
	nWorkers int
}

var _ types.BlockExecutor = &Executor{}

func NewExecutor(nWorkers int) *Executor {
	return &Executor{nWorkers: nWorkers}
}

func (e *Executor) Execute(block types.Block, startState types.AccountState) ([]types.AccountValue, error) {
	txExec := newTransactionExecutor(len(block.Transactions))
	optimisticExec := newOptimisticExecutor(&txExec, block.Transactions, startState)
	nodes := optimisticExec.execute(e.nWorkers)

	dag := NewDependencyDag(nodes)
	queue := newChannelProcessingQueue()
	delta := newAccountDelta(startState)

	dagExec := newDagExecutor(dag, &queue, &txExec, delta)
	dagExec.execute(e.nWorkers)

	return delta.UpdatedValues(), nil
}
