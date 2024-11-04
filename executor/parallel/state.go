package parallel

import (
	"blockchain/executor/api"
	"sync"
)

// executionAccountState is safe to use concurrently and supports writing changes
type executionAccountState struct {
	updatedState map[string]*api.AccountValue
	oldState     api.AccountState
	mu           sync.RWMutex
}

func newExecutionAccountState(oldState api.AccountState) *executionAccountState {
	return &executionAccountState{
		updatedState: make(map[string]*api.AccountValue),
		oldState:     oldState,
	}
}

func (s *executionAccountState) GetAccount(name string) api.AccountValue {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if v, ok := s.updatedState[name]; ok {
		return *v
	} else {
		return s.oldState.GetAccount(name)
	}
}

func (s *executionAccountState) ApplyUpdates(updates []api.AccountUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, update := range updates {
		s.write(update)
	}
}

func (s *executionAccountState) RevertUpdates(updates []api.AccountUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, update := range updates {
		inverseUpdate := api.AccountUpdate{
			Name:          update.Name,
			BalanceChange: -update.BalanceChange,
		}
		s.write(inverseUpdate)

		// Delete the entry from update state if it was reverted to default value,
		// so that we don't return extra entries from UpdatedValues func.
		if s.updatedState[update.Name].Balance == 0 {
			delete(s.updatedState, update.Name)
		}
	}
}

func (s *executionAccountState) write(update api.AccountUpdate) {
	v, ok := s.updatedState[update.Name]
	if ok {
		v.Balance += uint(update.BalanceChange)
	} else {
		s.updatedState[update.Name] = &api.AccountValue{
			Name:    update.Name,
			Balance: s.oldState.GetAccount(update.Name).Balance + uint(update.BalanceChange),
		}
	}
}

func (s *executionAccountState) UpdatedValues() []api.AccountValue {
	var updatedValues []api.AccountValue
	for name, accountValue := range s.updatedState {
		if s.oldState.GetAccount(name).Balance != accountValue.Balance {
			updatedValues = append(updatedValues, *accountValue)
		}
	}

	return updatedValues
}
