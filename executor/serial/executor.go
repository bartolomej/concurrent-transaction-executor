package serial

import (
	"blockchain/executor/api"
	"fmt"
)

type Executor struct{}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) ExecuteBlock(block api.Block, startState api.AccountState) ([]api.AccountValue, error) {
	blockState := newExecutorState(startState)

	for i, tx := range block.Transactions {
		updates, err := tx.Updates(blockState)
		if err == nil {
			blockState.ApplyUpdates(updates)
		} else {
			fmt.Printf("skipping failed transaction %d: %s\n", i, err)
		}
	}

	return blockState.UpdatedAccountValues(), nil
}

type executorState struct {
	uncommitedBalancesByName map[string]uint
	startState               api.AccountState
}

func newExecutorState(startState api.AccountState) *executorState {
	return &executorState{
		startState:               startState,
		uncommitedBalancesByName: make(map[string]uint),
	}
}

func (s *executorState) GetAccount(name string) api.AccountValue {
	_, ok := s.uncommitedBalancesByName[name]

	if !ok {
		s.initAccount(name)
	}

	return api.AccountValue{Name: name, Balance: s.uncommitedBalancesByName[name]}
}

func (s *executorState) ApplyUpdates(updates []api.AccountUpdate) {
	for _, u := range updates {
		_, ok := s.uncommitedBalancesByName[u.Name]
		if !ok {
			s.initAccount(u.Name)
		}
		s.uncommitedBalancesByName[u.Name] += uint(u.BalanceChange)
	}
}

func (s *executorState) initAccount(name string) {
	s.uncommitedBalancesByName[name] = s.startState.GetAccount(name).Balance
}

func (s *executorState) UpdatedAccountValues() []api.AccountValue {
	var updatedValues []api.AccountValue
	for k, v := range s.uncommitedBalancesByName {
		updatedValues = append(updatedValues, api.AccountValue{
			Name:    k,
			Balance: v,
		})
	}

	return updatedValues
}
