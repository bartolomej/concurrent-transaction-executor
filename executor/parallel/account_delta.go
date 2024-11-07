package parallel

import (
	"blockchain/executor/types"
	"cmp"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// accountDelta tracks the updated startState and supports writing and reverting startState changes
type accountDelta struct {
	updatedBalances       map[string]int
	oldState              types.AccountState
	appliedUpdatesBySeqId map[int][]types.AccountUpdate
	mu                    sync.RWMutex
}

func newAccountDelta(oldState types.AccountState) *accountDelta {
	return &accountDelta{
		updatedBalances:       make(map[string]int),
		appliedUpdatesBySeqId: make(map[int][]types.AccountUpdate),
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

	// Sanity check to detect possible bugs
	if len(s.appliedUpdatesBySeqId[seqId]) > 0 {
		// This should be caught in tests since it's a developer error.
		panic(fmt.Sprintf("updates were already applied for node %d", seqId))
	}
	for _, update := range updates {
		s.write(update)
	}
	s.appliedUpdatesBySeqId[seqId] = updates
}

func (s *accountDelta) RevertUpdates(seqId int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, update := range s.appliedUpdatesBySeqId[seqId] {
		inverseUpdate := types.AccountUpdate{
			Name:          update.Name,
			BalanceChange: -update.BalanceChange,
		}
		s.write(inverseUpdate)
	}

	delete(s.appliedUpdatesBySeqId, seqId)
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
	updatedState := s.UpdatedValues()
	slices.SortFunc(updatedState, func(a, b types.AccountValue) int {
		return cmp.Compare(a.Name, b.Name)
	})

	serUpdatedState := make([]string, 0, len(updatedState))

	for _, updated := range updatedState {
		serUpdatedState = append(serUpdatedState, fmt.Sprintf("(%s, %d)", updated.Name, updated.Balance))
	}

	return fmt.Sprintf("[%s]", strings.Join(serUpdatedState, ", "))
}
