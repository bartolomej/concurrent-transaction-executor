package executor

import (
	"fmt"
	"sync"
	"time"
)

type ParallelExecutor struct {
	NWorkers int
}

func NewParallelExecutor() *ParallelExecutor {
	return &ParallelExecutor{}
}

func (e *ParallelExecutor) ExecuteBlock(block Block, state AccountState) ([]AccountValue, error) {
	workers := e.initWorkers(block.Transactions)
	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		w := worker
		go func() {
			// TODO: Take into account executionReport
			w.execute()
			wg.Done()
		}()
	}
	panic("not implemented")
}

func (e *ParallelExecutor) initWorkers(transactions []Transaction) []executionWorker {
	var workers []executionWorker
	l := e.NWorkers
	n := len(transactions)
	for i := 0; i < n; i++ {
		start := i * l / n
		end := (i + 1) * l / n
		workers = append(workers, executionWorker{
			firstTxIndex: start,
			transactions: transactions[start:end],
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

func (e executionWorker) execute() []executionReport {
	var reports []executionReport
	index := e.firstTxIndex
	for _, transaction := range e.transactions {
		proxy := newStateProxy(e.state)
		updates, err := transaction.Updates(proxy)
		if err != nil {
			fmt.Printf("skipping failed transaction: %s\n", err)
			break
		}
		reports = append(reports, executionReport{
			index:       index,
			updates:     updates,
			reads:       proxy.reads,
			transaction: transaction,
		})
		index++
	}
	return reports
}

type stateProxy struct {
	reads []accountRead
	state AccountState
}

func newStateProxy(state AccountState) stateProxy {
	return stateProxy{state: state}
}

func (s stateProxy) GetAccount(name string) AccountValue {
	s.reads = append(s.reads, accountRead{
		name:      name,
		timestamp: time.Now().Unix(),
	})
	return s.state.GetAccount(name)
}
