package parallel

import (
	"blockchain/executor/api"
	"slices"
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
	nTx := len(block.Transactions)
	execNodeBatches := make([][]executionNode, e.NWorkers)

	wg := sync.WaitGroup{}
	for i := 0; i < e.NWorkers; i++ {
		startIdx := (nTx / e.NWorkers) * i
		endIdx := startIdx + (nTx / e.NWorkers)
		if i == e.NWorkers-1 {
			endIdx = nTx
		}

		wg.Add(1)
		go func(workerId int, wg *sync.WaitGroup) {
			result := make([]executionNode, 0, endIdx-startIdx)
			for idx := startIdx; idx < endIdx; idx++ {
				itx := indexedTransaction{
					seqId:       idx,
					transaction: &block.Transactions[idx],
				}
				// TODO: Avg execution time is still up to 2x larger than for serial processing,
				//  see if we can improve it (e.g. reduce scheduling overheat, allocation,...)
				result = append(result, execute(state, itx))
			}
			execNodeBatches[workerId] = result
			wg.Done()
		}(i, &wg)
	}

	wg.Wait()

	execNodes := make([]*executionNode, 0)
	for _, batch := range execNodeBatches {
		for _, node := range batch {
			execNodes = append(execNodes, &node)
		}
	}

	dag := newDependencyDag(execNodes)
	queue := newChannelProcessingQueue()
	execState := newConcurrentExecutionAccountState(state)

	for i := 0; i < e.NWorkers; i++ {
		go func(workerId int, queue <-chan processingUnit) {
			for visited := range queue {
				node := dag.lookup(visited.seqId)
				processUnit(node, dag, execState)
				visited.done()
			}
		}(i, queue.queue)
	}

	dag.concurrentWalk(queue)

	return execState.UpdatedValues(), nil
}

func processUnit(
	node *executionNode,
	dag *dependencyDag,
	state *concurrentExecutionAccountState,
) {
	// No need to re-execute the transaction,
	// as no other transaction can affect its dependencies
	// TODO: We need a reference of past relationships here, but we are removing nodes from the DAG
	//if len(dag.dependencies(node.seqId)) == 0 {
	//	state.WriteUpdates(node.updates)
	//	return
	//}

	reExecutedNode := execute(state, indexedTransaction{
		seqId:       node.seqId,
		transaction: node.transaction,
	})
	if slices.Equal(node.reads, reExecutedNode.reads) {
		// TODO: Writing reExecutedNode.updates will only work if there are no descending dependants on the new updates
		state.WriteUpdates(reExecutedNode.updates)
		return
	}

	// TODO: if reads or updates changed, update the dag to "invalidate" descendant nodes
	dag.update(&reExecutedNode)
}

type indexedTransaction struct {
	seqId       int
	transaction *api.Transaction
}

func execute(state api.AccountState, itx indexedTransaction) executionNode {
	proxy := stateProxy{readsLookup: make(map[string]bool), state: state}
	updates, err := (*itx.transaction).Updates(&proxy)

	return executionNode{
		seqId:       itx.seqId,
		updates:     updates,
		reads:       proxy.reads(),
		err:         err,
		transaction: itx.transaction,
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
	for read, _ := range s.readsLookup {
		reads = append(reads, read)
	}
	// Sort so that comparisons with slices.Equal work
	sort.Strings(reads)
	return reads
}

type concurrentExecutionAccountState struct {
	updatedState map[string]*api.AccountValue
	oldState     api.AccountState
	mu           sync.RWMutex
}

func newConcurrentExecutionAccountState(oldState api.AccountState) *concurrentExecutionAccountState {
	return &concurrentExecutionAccountState{
		updatedState: make(map[string]*api.AccountValue),
		oldState:     oldState,
	}
}

func (s *concurrentExecutionAccountState) GetAccount(name string) api.AccountValue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.updatedState[name]; ok {
		return *v
	} else {
		return s.oldState.GetAccount(name)
	}
}

func (s *concurrentExecutionAccountState) WriteUpdates(updates []api.AccountUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, update := range updates {
		v, ok := s.updatedState[update.Name]
		if ok {
			v.Balance += uint(update.BalanceChange)
		} else {
			s.updatedState[update.Name] = &api.AccountValue{
				Name:    update.Name,
				Balance: s.oldState.GetAccount(update.Name).Balance + uint(update.BalanceChange),
			}
		}
	}
}

func (s *concurrentExecutionAccountState) UpdatedValues() []api.AccountValue {
	var updatedValues []api.AccountValue
	for _, v := range s.updatedState {
		updatedValues = append(updatedValues, *v)
	}

	return updatedValues
}
