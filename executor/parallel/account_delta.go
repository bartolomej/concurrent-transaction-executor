package parallel

import (
	"blockchain/executor/api"
)

type accountDelta struct {
	updatedBalances map[string]int
	oldState        api.AccountState
}

func newAccountDelta(oldState api.AccountState) *accountDelta {
	return &accountDelta{
		updatedBalances: make(map[string]int),
		oldState:        oldState,
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

func (s *accountDelta) ApplyUpdates(updates []api.AccountUpdate) {
	for _, update := range updates {
		s.write(update)
	}
}

func (s *accountDelta) RevertUpdates(updates []api.AccountUpdate) {
	for _, update := range updates {
		inverseUpdate := api.AccountUpdate{
			Name:          update.Name,
			BalanceChange: -update.BalanceChange,
		}
		s.write(inverseUpdate)
	}
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
