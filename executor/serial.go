package executor

import (
	"fmt"
	"sort"
)

type SerialExecutor struct{}

func NewSerialExecutor() *SerialExecutor {
	return &SerialExecutor{}
}

func (e *SerialExecutor) ExecuteBlock(block Block, startState AccountState) ([]AccountValue, error) {
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
	startState               AccountState
}

func newExecutorState(startState AccountState) *executorState {
	return &executorState{
		startState:               startState,
		uncommitedBalancesByName: make(map[string]uint),
	}
}

func (s *executorState) GetAccount(name string) AccountValue {
	_, ok := s.uncommitedBalancesByName[name]

	if !ok {
		s.initAccount(name)
	}

	return AccountValue{Name: name, Balance: s.uncommitedBalancesByName[name]}
}

func (s *executorState) ApplyUpdates(updates []AccountUpdate) {
	for _, u := range updates {
		_, ok := s.uncommitedBalancesByName[u.Name]
		if !ok {
			s.initAccount(u.Name)
		}
		s.uncommitedBalancesByName[u.Name] += u.BalanceChange
	}
}

func (s *executorState) initAccount(name string) {
	s.uncommitedBalancesByName[name] = s.startState.GetAccount(name).Balance
}

func (s *executorState) UpdatedAccountValues() []AccountValue {
	var updatedValues []AccountValue
	for k, v := range s.uncommitedBalancesByName {
		updatedValues = append(updatedValues, AccountValue{
			Name:    k,
			Balance: v,
		})
	}

	// TODO: Does ExecuteBlock need to return entries in a specific order?
	sort.Slice(updatedValues, func(i, j int) bool {
		return updatedValues[i].Name < updatedValues[j].Name
	})

	return updatedValues
}
