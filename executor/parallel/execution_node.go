package parallel

import (
	"blockchain/executor/types"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type ExecutionNode struct {
	// SeqId is a unique identifier that also indicates the Transaction order within a block
	SeqId int
	// Reads account names that were read by the Transaction
	Reads []string
	// Updates account state changes that were produced by the Transaction
	Updates []types.AccountUpdate
	// no Updates were produced if Err is set
	Err error
	// Transaction is the state transition function that produces Reads, Updates, Err
	Transaction *types.Transaction
}

// executeTransaction executes the given Transaction and creates an execution node with the necessary runtime info
func executeTransaction(state types.AccountState, seqId int, transaction types.Transaction) *ExecutionNode {
	proxy := stateProxy{readsLookup: make(map[string]bool), state: state}
	updates, err := transaction.Updates(&proxy)

	return &ExecutionNode{
		SeqId:       seqId,
		Updates:     updates,
		Reads:       proxy.reads(),
		Err:         err,
		Transaction: &transaction,
	}
}

type stateProxy struct {
	readsLookup map[string]bool
	state       types.AccountState
}

var _ types.AccountState = &stateProxy{}

func (s *stateProxy) Get(name string) types.AccountValue {
	s.readsLookup[name] = true
	return s.state.Get(name)
}

func (s *stateProxy) reads() []string {
	reads := make([]string, 0, len(s.readsLookup))
	for read := range s.readsLookup {
		reads = append(reads, read)
	}
	// Sort so that comparisons with slices.Equal work
	sort.Strings(reads)
	return reads
}

func (node *ExecutionNode) String() string {
	readNames := make([]string, len(node.Reads))
	for i, read := range node.Reads {
		readNames[i] = read
	}

	updateEntries := make([]string, len(node.Updates))
	for i, update := range node.Updates {
		updateEntries[i] = update.Name + ":" + strconv.FormatInt(int64(update.BalanceChange), 10)
	}

	return fmt.Sprintf(
		"ExecutionNode{nodeSeqId:%d,Reads:%s,Updates:%s,Err:%v}",
		node.SeqId,
		"("+strings.Join(readNames, ",")+")",
		"("+strings.Join(updateEntries, ",")+")",
		node.Err,
	)
}