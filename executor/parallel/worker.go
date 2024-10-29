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
	state executor.AccountState
}

type indexedTransaction struct {
	index       int
	transaction executor.Transaction
}

func (e executionWorker) execute(
	transactions <-chan indexedTransaction,
	reports chan<- executionReport,
) {
	for tx := range transactions {
		proxy := newStateProxy(e.state)
		updates, err := tx.transaction.Updates(proxy)
		reports <- executionReport{
			index:   tx.index,
			updates: updates,
			reads:   proxy.reads,
			err:     err,
		}
	}
}

type accountRead struct {
	name      string
	timestamp int64
}

type executionReport struct {
	index int
	// reads account names that were read by the transaction
	reads []string
	// updates account state changes that were produced by the transaction
	updates []executor.AccountUpdate
	// no updates were produced if err is set
	err error
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
