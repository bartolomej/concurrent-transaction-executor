package parallel

import (
	"blockchain/executor"
	"fmt"
	"slices"
	"sync"
)

type Executor struct {
	NWorkers int
}

func NewExecutor(nWorkers int) *Executor {
	return &Executor{NWorkers: nWorkers}
}

func (e *Executor) ExecuteBlock(block executor.Block, state executor.AccountState) ([]executor.AccountValue, error) {
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

	reports := make([]executionReport, 0)
	for report := range executionReports {
		reports = append(reports, report)
	}
	slices.SortFunc(reports, func(a, b executionReport) int {
		return a.index - b.index
	})
	dag := buildDependencyDag(reports)

	fmt.Println("DAG")
	fmt.Println(dag)

	panic("not implemented")
}

// buildDependencyDag produces a dependency DAG (Directed Acyclic Graph)
// given an ordered (by index) list of execution reports
func buildDependencyDag(reports []executionReport) *dependencyDag {
	nodesByUpdate := make(map[string]map[int]bool)
	dag := newDependencyDag()
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

func (e *Executor) initWorkers(transactions []executor.Transaction, state executor.AccountState) []executionWorker {
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
