package serial

import (
	"blockchain/executor"
	"fmt"
	"sort"
)

type Executor struct{}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) ExecuteBlock(block executor.Block, startState executor.AccountState) ([]executor.AccountValue, error) {
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
	startState               executor.AccountState
}

func newExecutorState(startState executor.AccountState) *executorState {
	return &executorState{
		startState:               startState,
		uncommitedBalancesByName: make(map[string]uint),
	}
}

func (s *executorState) GetAccount(name string) executor.AccountValue {
	_, ok := s.uncommitedBalancesByName[name]

	if !ok {
		s.initAccount(name)
	}

	return executor.AccountValue{Name: name, Balance: s.uncommitedBalancesByName[name]}
}

func (s *executorState) ApplyUpdates(updates []executor.AccountUpdate) {
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

func (s *executorState) UpdatedAccountValues() []executor.AccountValue {
	var updatedValues []executor.AccountValue
	for k, v := range s.uncommitedBalancesByName {
		updatedValues = append(updatedValues, executor.AccountValue{
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
