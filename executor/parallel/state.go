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

func (s *executionAccountState) WriteUpdates(updates []api.AccountUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, update := range updates {
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
}

func (s *executionAccountState) UpdatedValues() []api.AccountValue {
	var updatedValues []api.AccountValue
	for _, v := range s.updatedState {
		updatedValues = append(updatedValues, *v)
	}

	return updatedValues
}
