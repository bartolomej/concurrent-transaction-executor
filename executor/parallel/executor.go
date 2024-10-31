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
	indexedTxs := make(chan indexedTransaction)
	executionNodes := make(chan *executionNode)

	wg := sync.WaitGroup{}
	for i := 0; i < e.NWorkers; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			for itx := range indexedTxs {
				// Send on closed channel happens here
				executionNodes <- execute(state, itx)
			}
			wg.Done()
		}(&wg)
	}

	go func() {
		for i, tx := range block.Transactions {
			indexedTxs <- indexedTransaction{
				index:       i,
				transaction: tx,
			}
		}
		close(indexedTxs)
	}()

	go func() {
		wg.Wait()
		close(executionNodes)
	}()

	serExecNodes := make([]*executionNode, 0)
	for node := range executionNodes {
		serExecNodes = append(serExecNodes, node)
	}

	dag := newDependencyDag(serExecNodes)
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
		index:       node.seqId,
		transaction: node.transaction,
	})
	if slices.Equal(node.reads, reExecutedNode.reads) {
		// TODO: Writing reExecutedNode.updates will only work if there are no descending dependants on the new updates
		state.WriteUpdates(reExecutedNode.updates)
		return
	}

	// TODO: if reads or updates changed, update the dag to "invalidate" descendant nodes
	dag.update(reExecutedNode)
}

type indexedTransaction struct {
	index       int
	transaction api.Transaction
}

func execute(state api.AccountState, tx indexedTransaction) *executionNode {
	proxy := newStateProxy(state)
	updates, err := tx.transaction.Updates(proxy)
	return &executionNode{
		seqId:       tx.index,
		updates:     updates,
		reads:       proxy.reads(),
		err:         err,
		transaction: tx.transaction,
	}
}

type stateProxy struct {
	readsLookup map[string]bool
	state       api.AccountState
}

func newStateProxy(state api.AccountState) *stateProxy {
	return &stateProxy{readsLookup: make(map[string]bool), state: state}
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

	// TODO: Does ExecuteBlock need to return entries in a specific order?
	sort.Slice(updatedValues, func(i, j int) bool {
		return updatedValues[i].Name < updatedValues[j].Name
	})

	return updatedValues
}
