package parallel

import (
	"blockchain/executor/types"
	"sync"
)

type Executor struct {
	nWorkers int
}

var _ types.BlockExecutor = &Executor{}

func NewExecutor(nWorkers int) *Executor {
	return &Executor{nWorkers: nWorkers}
}

func (e *Executor) Execute(block types.Block, state types.AccountState) ([]types.AccountValue, error) {
	nodes := e.executeOptimistically(block.Transactions, state)

	dag := NewDependencyDag(nodes)
	delta := newAccountDelta(state)

	dag.Execute(delta, e.nWorkers)

	return delta.UpdatedValues(), nil
}

func (e *Executor) executeOptimistically(transactions []types.Transaction, state types.AccountState) []*ExecutionNode {
	nTx := len(transactions)
	nodeBatches := make([][]*ExecutionNode, e.nWorkers)

	wg := sync.WaitGroup{}
	for workerId := 0; workerId < e.nWorkers; workerId++ {
		chunk := nTx / e.nWorkers
		startSeqId := chunk * workerId
		endSeqId := startSeqId + chunk
		if workerId == e.nWorkers-1 {
			endSeqId = nTx
		}

		wg.Add(1)
		go func(workerId, startSeqId, endSeqId int, wg *sync.WaitGroup) {
			nodeBatch := make([]*ExecutionNode, 0, endSeqId-startSeqId)
			for seqId := startSeqId; seqId < endSeqId; seqId++ {
				nodeBatch = append(nodeBatch, executeTransaction(state, seqId, transactions[seqId]))
			}
			nodeBatches[workerId] = nodeBatch
			wg.Done()
		}(workerId, startSeqId, endSeqId, &wg)
	}

	wg.Wait()

	nodes := make([]*ExecutionNode, 0, len(transactions))
	for _, batch := range nodeBatches {
		for _, node := range batch {
			nodes = append(nodes, node)
		}
	}

	return nodes
}
