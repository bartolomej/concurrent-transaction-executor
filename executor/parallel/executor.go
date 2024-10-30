package parallel

import (
	"blockchain/executor"
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

func (e *Executor) ExecuteBlock(block executor.Block, state executor.AccountState) ([]executor.AccountValue, error) {
	transactionsQueue := make(chan indexedTransaction)
	executionReports := make(chan executionReport)

	wg := sync.WaitGroup{}
	for i := 0; i < e.NWorkers; i++ {
		worker := executionWorker{state: state}
		go func(wg *sync.WaitGroup) {
			wg.Add(1)
			worker.execute(transactionsQueue, executionReports)
			wg.Done()
		}(&wg)
	}

	for i, tx := range block.Transactions {
		transactionsQueue <- indexedTransaction{
			index:       i,
			transaction: tx,
		}
	}
	close(transactionsQueue)

	go func() {
		wg.Wait()
		close(executionReports)
	}()

	reports := make([]executionReport, 0)
	for report := range executionReports {
		reports = append(reports, report)
	}
	slices.SortFunc(reports, func(a, b executionReport) int {
		return a.index - b.index
	})

	dag := buildDependencyDag(reports)
	processingQueue := newDagChannelQueue()
	executionState := newConcurrentExecutionAccountState(state)

	for i := 0; i < e.NWorkers; i++ {
		go func(workerId int, queue <-chan dagQueueElement) {
			for visited := range queue {
				node := dag.lookup(visited.nodeId)
				report := reports[visited.nodeId]
				tx := block.Transactions[visited.nodeId]
				processDagQueueElement(node, report, tx, executionState)
				visited.wg.Done()
			}
		}(i, processingQueue.channel)
	}

	dag.concurrentWalk(processingQueue)

	return executionState.UpdatedValues(), nil
}

func processDagQueueElement(
	node *dependencyNode,
	report executionReport,
	tx executor.Transaction,
	executionState *concurrentExecutionAccountState,
) {
	iTx := indexedTransaction{
		index:       report.index,
		transaction: tx,
	}

	// No need to re-execute the transaction,
	// as no other transaction can affect its dependencies
	if len(node.Dependencies) == 0 {
		executionState.WriteUpdates(report.updates)
		return
	}

	newReport := executeAndReport(executionState, iTx)
	if slices.Equal(report.reads, newReport.reads) {
		// TODO: Writing newReport.updates will only work if there are no descending dependants on the new updates
		executionState.WriteUpdates(newReport.updates)
		return
	}

	// TODO: if reads or updates changed, update the dag to "invalidate" descendant nodes
	panic("unimplemented")
}

type concurrentExecutionAccountState struct {
	updatedState map[string]*executor.AccountValue
	oldState     executor.AccountState
	mu           sync.RWMutex
}

func newConcurrentExecutionAccountState(oldState executor.AccountState) *concurrentExecutionAccountState {
	return &concurrentExecutionAccountState{
		updatedState: make(map[string]*executor.AccountValue),
		oldState:     oldState,
	}
}

func (s *concurrentExecutionAccountState) GetAccount(name string) executor.AccountValue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.updatedState[name]; ok {
		return *v
	} else {
		return s.oldState.GetAccount(name)
	}
}

func (s *concurrentExecutionAccountState) WriteUpdates(updates []executor.AccountUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, update := range updates {
		v, ok := s.updatedState[update.Name]
		if ok {
			v.Balance += uint(update.BalanceChange)
		} else {
			s.updatedState[update.Name] = &executor.AccountValue{
				Name:    update.Name,
				Balance: s.oldState.GetAccount(update.Name).Balance + uint(update.BalanceChange),
			}
		}
	}
}

func (s *concurrentExecutionAccountState) UpdatedValues() []executor.AccountValue {
	var updatedValues []executor.AccountValue
	for _, v := range s.updatedState {
		updatedValues = append(updatedValues, *v)
	}

	// TODO: Does ExecuteBlock need to return entries in a specific order?
	sort.Slice(updatedValues, func(i, j int) bool {
		return updatedValues[i].Name < updatedValues[j].Name
	})

	return updatedValues
}

// buildDependencyDag produces a dependency DAG (Directed Acyclic Graph)
// given an ordered (by index) list of execution reports
func buildDependencyDag(reports []executionReport) *dependencyDag {
	nodesByUpdate := make(map[string]map[int]bool)
	dag := newDependencyDag(nil)
	for _, report := range reports {
		var dependencies []int
		for _, read := range report.reads {
			for id, _ := range nodesByUpdate[read] {
				dependencies = append(dependencies, id)
			}
		}
		dag.add(newDependencyNode(report.index, dependencies))
		for _, update := range report.updates {
			_, ok := nodesByUpdate[update.Name]
			if !ok {
				nodesByUpdate[update.Name] = make(map[int]bool)
			}
			nodesByUpdate[update.Name][report.index] = true
		}
	}

	return dag
}
