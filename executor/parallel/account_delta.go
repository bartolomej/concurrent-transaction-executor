package parallel

import (
	"blockchain/executor/api"
	"fmt"
	"strings"
)

type accountDelta struct {
	updatedBalances       map[string]int
	oldState              api.AccountState
	appliedUpdatesBySeqId map[int]bool
}

func newAccountDelta(oldState api.AccountState) *accountDelta {
	return &accountDelta{
		updatedBalances:       make(map[string]int),
		appliedUpdatesBySeqId: make(map[int]bool),
		oldState:              oldState,
	}
}

func (s *accountDelta) GetAccount(name string) api.AccountValue {
	if balance, ok := s.updatedBalances[name]; ok {
		return api.AccountValue{
			Name:    name,
			Balance: uint(balance),
		}
	} else {
		return s.oldState.GetAccount(name)
	}
}

func (s *accountDelta) ApplyUpdates(seqId int, updates []api.AccountUpdate) {
	for _, update := range updates {
		s.write(update)
	}
	s.appliedUpdatesBySeqId[seqId] = true
}

func (s *accountDelta) RevertUpdates(seqId int, updates []api.AccountUpdate) {
	if !s.appliedUpdatesBySeqId[seqId] {
		return
	}
	for _, update := range updates {
		inverseUpdate := api.AccountUpdate{
			Name:          update.Name,
			BalanceChange: -update.BalanceChange,
		}
		s.write(inverseUpdate)
	}
	s.appliedUpdatesBySeqId[seqId] = false
}

func (s *accountDelta) write(update api.AccountUpdate) {
	_, ok := s.updatedBalances[update.Name]
	if ok {
		s.updatedBalances[update.Name] += update.BalanceChange
	} else {
		s.updatedBalances[update.Name] = int(s.oldState.GetAccount(update.Name).Balance) + update.BalanceChange
	}
}

func (s *accountDelta) UpdatedValues() []api.AccountValue {
	var updatedValues []api.AccountValue
	for name, updatedBalance := range s.updatedBalances {
		if s.oldState.GetAccount(name).Balance != uint(updatedBalance) {
			updatedValues = append(updatedValues, api.AccountValue{
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
		"delta{updatedState: %s, updatesFrom: %s}",
		strings.Join(serUpdatedState, ", "),
		strings.Join(serSeqIds, ", "),
	)
}
