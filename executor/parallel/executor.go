package parallel

import (
	"blockchain/executor/api"
	"sort"
	"sync"
)

type Executor struct {
	NWorkers int
}

func NewExecutor(nWorkers int) *Executor {
	return &Executor{NWorkers: nWorkers}
}

func (e *Executor) ExecuteBlock(block api.Block, state api.AccountState) ([]api.AccountValue, error) {
	nodes := e.executeOptimistically(block.Transactions, state)

	dag := newDependencyDag(nodes)
	execState := newExecutionAccountState(state)

	dag.execute(execState, e.NWorkers)

	return execState.UpdatedValues(), nil
}

func (e *Executor) executeOptimistically(transactions []api.Transaction, state api.AccountState) []*executionNode {
	nTx := len(transactions)
	nodeBatches := make([][]*executionNode, e.NWorkers)

	wg := sync.WaitGroup{}
	for workerId := 0; workerId < e.NWorkers; workerId++ {
		chunk := nTx / e.NWorkers
		startSeqId := chunk * workerId
		endSeqId := startSeqId + chunk
		if workerId == e.NWorkers-1 {
			endSeqId = nTx
		}

		wg.Add(1)
		go func(workerId, startSeqId, endSeqId int, wg *sync.WaitGroup) {
			nodeBatch := make([]*executionNode, 0, endSeqId-startSeqId)
			for seqId := startSeqId; seqId < endSeqId; seqId++ {
				// TODO(perf): Avg execution time is sometimes still up to 2x larger than for serial processing,
				//  see if we can improve it (e.g. reduce scheduling overheat, allocation,...)
				nodeBatch = append(nodeBatch, executeTransaction(state, seqId, transactions[seqId]))
			}
			nodeBatches[workerId] = nodeBatch
			wg.Done()
		}(workerId, startSeqId, endSeqId, &wg)
	}

	wg.Wait()

	nodes := make([]*executionNode, 0, len(transactions))
	for _, batch := range nodeBatches {
		for _, node := range batch {
			nodes = append(nodes, node)
		}
	}

	return nodes
}

func executeTransaction(state api.AccountState, seqId int, transaction api.Transaction) *executionNode {
	proxy := stateProxy{readsLookup: make(map[string]bool), state: state}
	updates, err := transaction.Updates(&proxy)

	return &executionNode{
		seqId:       seqId,
		updates:     updates,
		reads:       proxy.reads(),
		err:         err,
		transaction: &transaction,
	}
}

type stateProxy struct {
	readsLookup map[string]bool
	state       api.AccountState
}

func (s *stateProxy) GetAccount(name string) api.AccountValue {
	s.readsLookup[name] = true
	return s.state.GetAccount(name)
}

func (s *stateProxy) reads() []string {
	reads := make([]string, 0, len(s.readsLookup))
	for read := range s.readsLookup {
		reads = append(reads, read)
	}
	// Sort so that comparisons with slices.Equal work
	sort.Strings(reads)
	return reads
}
