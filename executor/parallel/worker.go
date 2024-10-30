package parallel

import (
	"blockchain/executor"
	"fmt"
	"sort"
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
			reads:   proxy.reads(),
			err:     err,
		}
	}
}

func executeAndReport(state executor.AccountState, tx indexedTransaction) executionReport {
	proxy := newStateProxy(state)
	updates, err := tx.transaction.Updates(proxy)
	return executionReport{
		index:   tx.index,
		updates: updates,
		reads:   proxy.reads(),
		err:     err,
	}
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
		"executionReport{index: %d, readsLookup: %s, updates: %s}",
		r.index,
		"("+strings.Join(readNames, ",")+")",
		"("+strings.Join(updateEntries, ",")+")",
	)
}

type stateProxy struct {
	readsLookup map[string]bool
	state       executor.AccountState
}

func newStateProxy(state executor.AccountState) *stateProxy {
	return &stateProxy{readsLookup: make(map[string]bool), state: state}
}

func (s *stateProxy) GetAccount(name string) executor.AccountValue {
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
