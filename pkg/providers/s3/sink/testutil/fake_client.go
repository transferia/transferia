package testutil

import (
	"github.com/transferria/transferria/pkg/abstract/coordinator"
)

// FakeClientWithTransferState is a fake controlplane client which stores sharded object transfer state
type FakeClientWithTransferState struct {
	coordinator.CoordinatorNoOp
	state map[string]*coordinator.TransferStateData
}

func (c *FakeClientWithTransferState) SetTransferState(transferID string, state map[string]*coordinator.TransferStateData) error {
	c.state = state
	return nil
}

func (c *FakeClientWithTransferState) GetTransferState(transferID string) (map[string]*coordinator.TransferStateData, error) {
	return c.state, nil
}

func NewFakeClientWithTransferState() *FakeClientWithTransferState {
	return &FakeClientWithTransferState{
		CoordinatorNoOp: coordinator.CoordinatorNoOp{},
		state:           nil,
	}
}
