package parallel

import (
	"blockchain/executor/types"
	"sync"
)

// optimisticExecutor executes all given transactions concurrently against the initial startState
type optimisticExecutor struct {
	txExecutor   *transactionExecutor
	transactions []types.Transaction
	startState   types.AccountState
}

func newOptimisticExecutor(
	txExecutor *transactionExecutor,
	transactions []types.Transaction,
	state types.AccountState,
) optimisticExecutor {
	return optimisticExecutor{
		txExecutor,
		transactions,
		state,
	}
}

func (e *optimisticExecutor) execute(nWorkers int) []*ExecutionNode {
	nTx := len(e.transactions)
	nodeBatches := make([][]*ExecutionNode, nWorkers)

	wg := sync.WaitGroup{}
	for workerId := 0; workerId < nWorkers; workerId++ {
		chunk := nTx / nWorkers
		startSeqId := chunk * workerId
		endSeqId := startSeqId + chunk
		if workerId == nWorkers-1 {
			endSeqId = nTx
		}

		wg.Add(1)
		go func(workerId, startSeqId, endSeqId int, wg *sync.WaitGroup) {
			nodeBatch := make([]*ExecutionNode, 0, endSeqId-startSeqId)
			for seqId := startSeqId; seqId < endSeqId; seqId++ {
				nodeBatch = append(nodeBatch, e.txExecutor.execute(e.startState, seqId, e.transactions[seqId]))
			}
			nodeBatches[workerId] = nodeBatch
			wg.Done()
		}(workerId, startSeqId, endSeqId, &wg)
	}

	wg.Wait()

	nodes := make([]*ExecutionNode, 0, len(e.transactions))
	for _, batch := range nodeBatches {
		for _, node := range batch {
			nodes = append(nodes, node)
		}
	}

	return nodes
}
