package parallel

import (
	"blockchain/executor"
	"fmt"
	"slices"
	"sync"
	"time"
)

type Executor struct {
	NWorkers int
}

func NewExecutor(nWorkers int) *Executor {
	return &Executor{NWorkers: nWorkers}
}

func (e *Executor) ExecuteBlock(block executor.Block, state executor.AccountState) ([]executor.AccountValue, error) {
	// TODO: Handle when NWorkers > len(block.Transactions)
	// TODO: Refactor initWorkers to be more idiomatic and rely on channels
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

	processingQueue := &dagChannelQueue{channel: make(chan dagQueueElement, e.NWorkers)}

	for i := 0; i < e.NWorkers; i++ {
		go func(workerId int, queue <-chan dagQueueElement) {
			element := <-queue
			fmt.Printf("worker %d processing node: %d\n", workerId, element.nodeId)
			time.Sleep(time.Second)
			element.wg.Done()

		}(i, processingQueue.channel)
	}

	dag.concurrentWalk(processingQueue)

	panic("not implemented")
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
