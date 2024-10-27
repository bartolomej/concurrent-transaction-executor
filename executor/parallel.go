package executor

import (
	"fmt"
	"sync"
	"time"
)

type ParallelExecutor struct {
	NWorkers int
}

func NewParallelExecutor(nWorkers int) *ParallelExecutor {
	return &ParallelExecutor{NWorkers: nWorkers}
}

func (e *ParallelExecutor) ExecuteBlock(block Block, state AccountState) ([]AccountValue, error) {
	workers := e.initWorkers(block.Transactions, state)
	executionReports := make(chan executionReport)

	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go func(w executionWorker) {
			w.execute(executionReports)
			wg.Done()
		}(worker)
	}
	go func() {
		wg.Wait()
		close(executionReports)
	}()

	for report := range executionReports {
		fmt.Printf("execution report %s\n", report)
	}
	panic("not implemented")
}

func (e *ParallelExecutor) initWorkers(transactions []Transaction, state AccountState) []executionWorker {
	var workers []executionWorker
	l := e.NWorkers
	n := len(transactions)
	for i := 0; i < n; i++ {
		start := i * l / n
		end := (i + 1) * l / n
		workers = append(workers, executionWorker{
			firstTxIndex: start,
			transactions: transactions[start:end],
			state:        state,
		})
	}
	return workers
}

type executionWorker struct {
	firstTxIndex int
	transactions []Transaction
	state        AccountState
}

type accountRead struct {
	name      string
	timestamp int64
}

type executionReport struct {
	index       int
	reads       []accountRead
	updates     []AccountUpdate
	transaction Transaction
}

func (r executionReport) String() string {
	return fmt.Sprintf(
		"executionReport{index: %d, reads: %d, updates: %d}",
		r.index, len(r.reads), len(r.updates),
	)
}

func (e executionWorker) execute(executionReports chan executionReport) {
	index := e.firstTxIndex
	for _, transaction := range e.transactions {
		proxy := newStateProxy(e.state)
		updates, err := transaction.Updates(proxy)
		if err != nil {
			fmt.Printf("skipping failed transaction: %s\n", err)
			break
		}
		executionReports <- executionReport{
			index:       index,
			updates:     updates,
			reads:       proxy.reads,
			transaction: transaction,
		}
		index++
	}
}

type stateProxy struct {
	reads []accountRead
	state AccountState
}

func newStateProxy(state AccountState) *stateProxy {
	return &stateProxy{state: state}
}

func (s *stateProxy) GetAccount(name string) AccountValue {
	s.reads = append(s.reads, accountRead{
		name:      name,
		timestamp: time.Now().Unix(),
	})
	return s.state.GetAccount(name)
}
