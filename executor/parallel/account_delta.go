package parallel

import (
	"blockchain/executor/types"
	"fmt"
	"strings"
	"sync"
)

// accountDelta keeps track of the updated state and supports writing and reverting state changes
type accountDelta struct {
	updatedBalances       map[string]int
	oldState              types.AccountState
	appliedUpdatesBySeqId map[int]bool
	mu                    sync.RWMutex
}

func newAccountDelta(oldState types.AccountState) *accountDelta {
	return &accountDelta{
		updatedBalances:       make(map[string]int),
		appliedUpdatesBySeqId: make(map[int]bool),
		oldState:              oldState,
	}
}

func (s *accountDelta) Get(name string) types.AccountValue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if balance, ok := s.updatedBalances[name]; ok {
		return types.AccountValue{
			Name:    name,
			Balance: uint(balance),
		}
	} else {
		return s.oldState.Get(name)
	}
}

func (s *accountDelta) ApplyUpdates(seqId int, updates []types.AccountUpdate) {
	if len(updates) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.appliedUpdatesBySeqId[seqId] {
		panic(fmt.Sprintf("updates were already applied for node %d", seqId))
	}
	for _, update := range updates {
		s.write(update)
	}
	s.appliedUpdatesBySeqId[seqId] = true
}

func (s *accountDelta) RevertUpdates(seqId int, updates []types.AccountUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.appliedUpdatesBySeqId[seqId] {
		return
	}
	for _, update := range updates {
		inverseUpdate := types.AccountUpdate{
			Name:          update.Name,
			BalanceChange: -update.BalanceChange,
		}
		s.write(inverseUpdate)
	}
	s.appliedUpdatesBySeqId[seqId] = false
}

func (s *accountDelta) write(update types.AccountUpdate) {
	_, ok := s.updatedBalances[update.Name]
	if ok {
		s.updatedBalances[update.Name] += update.BalanceChange
	} else {
		s.updatedBalances[update.Name] = int(s.oldState.Get(update.Name).Balance) + update.BalanceChange
	}
}

func (s *accountDelta) UpdatedValues() []types.AccountValue {
	var updatedValues []types.AccountValue
	for name, updatedBalance := range s.updatedBalances {
		if s.oldState.Get(name).Balance != uint(updatedBalance) {
			updatedValues = append(updatedValues, types.AccountValue{
				Name:    name,
				Balance: uint(updatedBalance),
			})
		}
	}

	return updatedValues
}

func (s *accountDelta) String() string {
	serUpdatedState := make([]string, 0, len(s.updatedBalances))
	for name, balance := range s.updatedBalances {
		serUpdatedState = append(serUpdatedState, fmt.Sprintf("(%s, %d)", name, balance))
	}
	serSeqIds := make([]string, 0, len(s.appliedUpdatesBySeqId))
	for seqId, isApplied := range s.appliedUpdatesBySeqId {
		if isApplied {
			serSeqIds = append(serSeqIds, fmt.Sprintf("%d", seqId))
		}
	}
	return fmt.Sprintf(
		"accountDelta{updatedState: %s, updatesFrom: %s}",
		strings.Join(serUpdatedState, ", "),
		strings.Join(serSeqIds, ", "),
	)
}
