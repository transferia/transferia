package testutil

import (
	"sort"
	"testing"

	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"golang.org/x/exp/maps"
)

// FakeClientWithTransferState is a fake controlplane client which stores sharded object transfer state
type FakeClientWithTransferState struct {
	coordinator.CoordinatorNoOp
	state map[string]*coordinator.TransferStateData
}

func (c *FakeClientWithTransferState) SetTransferState(transferID string, inSstate map[string]*coordinator.TransferStateData) error {
	for k, v := range inSstate {
		c.state[k] = v
	}
	return nil
}

func (c *FakeClientWithTransferState) GetTransferState(transferID string) (map[string]*coordinator.TransferStateData, error) {
	return c.state, nil
}

func (c *FakeClientWithTransferState) StateKeys() []string {
	stateKeys := maps.Keys(c.state)
	sort.Strings(stateKeys)
	return stateKeys
}

func (c *FakeClientWithTransferState) GetTransferStateForTests(t *testing.T) map[string]*coordinator.TransferStateData {
	return c.state
}

func NewFakeClientWithTransferState() *FakeClientWithTransferState {
	return &FakeClientWithTransferState{
		CoordinatorNoOp: coordinator.CoordinatorNoOp{},
		state:           make(map[string]*coordinator.TransferStateData),
	}
}
