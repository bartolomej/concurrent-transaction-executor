package executor

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
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

	dag := buildDependencyDag(executionReports)

	fmt.Println("DAG")
	fmt.Println(dag)

	panic("not implemented")
}

// DependencyDag is a Directed Acyclic Graph that is not necessarily connected.
type DependencyDag struct {
	nodes map[int]*DependencyNode
}

func newDependencyDag() *DependencyDag {
	return &DependencyDag{nodes: make(map[int]*DependencyNode)}
}

func (dag DependencyDag) add(node *DependencyNode) {
	dag.nodes[node.Id] = node
}

func (dag DependencyDag) lookup(id int) *DependencyNode {
	return dag.nodes[id]
}

func (dag DependencyDag) String() string {
	sb := strings.Builder{}
	for _, node := range dag.nodes {
		sb.WriteString(node.String())
		sb.WriteString("\n")
	}
	return sb.String()
}

type DependencyNode struct {
	Id              int
	Dependencies    []*DependencyNode
	ExecutionReport executionReport
}

func (node DependencyNode) String() string {
	deps := make([]string, len(node.Dependencies))
	for i, dep := range node.Dependencies {
		deps[i] = strconv.Itoa(dep.Id)
	}
	return fmt.Sprintf(
		"Dependency{id:%s,deps:(%s)}",
		strconv.Itoa(node.Id),
		strings.Join(deps, ","),
	)
}

func newDependencyNode(id int, dependencies []*DependencyNode, report executionReport) *DependencyNode {
	return &DependencyNode{
		Id:              id,
		Dependencies:    dependencies,
		ExecutionReport: report,
	}
}

func buildDependencyDag(executionReports <-chan executionReport) *DependencyDag {
	reports := make([]executionReport, 0)
	for report := range executionReports {
		fmt.Printf("execution report %s\n", report)
		reports = append(reports, report)
	}
	slices.SortFunc(reports, func(a, b executionReport) int {
		return a.index - b.index
	})

	nodesByUpdate := make(map[string]map[int]bool)
	dag := newDependencyDag()
	for _, report := range reports {
		var dependencies []*DependencyNode
		for _, read := range report.reads {
			for id, _ := range nodesByUpdate[read.name] {
				dependencies = append(dependencies, dag.lookup(id))
			}
		}
		dag.add(newDependencyNode(report.index, dependencies, report))
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
	readNames := make([]string, len(r.reads))
	for i, read := range r.reads {
		readNames[i] = read.name
	}

	updateEntries := make([]string, len(r.updates))
	for i, update := range r.updates {
		updateEntries[i] = update.Name + ":" + strconv.FormatInt(int64(update.BalanceChange), 10)
	}

	return fmt.Sprintf(
		"executionReport{index: %d, reads: %s, updates: %s}",
		r.index,
		"("+strings.Join(readNames, ",")+")",
		"("+strings.Join(updateEntries, ",")+")",
	)
}

func (e executionWorker) execute(executionReports chan<- executionReport) {
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
