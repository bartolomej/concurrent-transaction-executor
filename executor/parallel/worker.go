package parallel

import (
	"blockchain/executor"
	"fmt"
	"strconv"
	"strings"
)

// executionWorker executes a set of transactions given state
// and produces an executionReport for each executed transaction
type executionWorker struct {
	firstTxIndex int
	transactions []executor.Transaction
	state        executor.AccountState
}

func (e executionWorker) execute(reports chan<- executionReport) {
	index := e.firstTxIndex
	for _, transaction := range e.transactions {
		proxy := newStateProxy(e.state)
		updates, err := transaction.Updates(proxy)
		if err != nil {
			fmt.Printf("skipping failed transaction: %s\n", err)
			break
		}
		reports <- executionReport{
			index:   index,
			updates: updates,
			reads:   proxy.reads,
		}
		index++
	}
}

type accountRead struct {
	name      string
	timestamp int64
}

type executionReport struct {
	index int
	// reads stores account names that were read by the transaction
	reads []string
	// updates stores account state changes that were produced by the transaction
	updates []executor.AccountUpdate
}

func (r executionReport) String() string {
	readNames := make([]string, len(r.reads))
	for i, read := range r.reads {
		readNames[i] = read
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

type stateProxy struct {
	reads []string
	state executor.AccountState
}

func newStateProxy(state executor.AccountState) *stateProxy {
	return &stateProxy{state: state}
}

func (s *stateProxy) GetAccount(name string) executor.AccountValue {
	s.reads = append(s.reads, name)
	return s.state.GetAccount(name)
}
