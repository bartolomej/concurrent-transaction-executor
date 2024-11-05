package serial

import (
	"blockchain/executor/types"
	"fmt"
)

type Executor struct{}

var _ types.BlockExecutor = &Executor{}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) Execute(block types.Block, startState types.AccountState) ([]types.AccountValue, error) {
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
	startState               types.AccountState
}

func newExecutorState(startState types.AccountState) *executorState {
	return &executorState{
		startState:               startState,
		uncommitedBalancesByName: make(map[string]uint),
	}
}

func (s *executorState) Get(name string) types.AccountValue {
	_, ok := s.uncommitedBalancesByName[name]

	if !ok {
		return s.startState.Get(name)
	}

	return types.AccountValue{Name: name, Balance: s.uncommitedBalancesByName[name]}
}

func (s *executorState) ApplyUpdates(updates []types.AccountUpdate) {
	for _, u := range updates {
		_, ok := s.uncommitedBalancesByName[u.Name]
		if !ok {
			s.uncommitedBalancesByName[u.Name] = s.startState.Get(u.Name).Balance
		}
		s.uncommitedBalancesByName[u.Name] += uint(u.BalanceChange)
	}
}

func (s *executorState) UpdatedAccountValues() []types.AccountValue {
	var updatedValues []types.AccountValue
	for k, v := range s.uncommitedBalancesByName {
		if s.startState.Get(k).Balance != v {
			updatedValues = append(updatedValues, types.AccountValue{
				Name:    k,
				Balance: v,
			})
		}
	}

	return updatedValues
}
