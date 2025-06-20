//go:build !disable_kinesis_provider

package consumer

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

func NewCoordinatorStore(cp coordinator.TransferState, transferID string) *CoordinatorStore {
	return &CoordinatorStore{
		cp:         cp,
		transferID: transferID,
	}
}

type CoordinatorStore struct {
	cp         coordinator.TransferState
	transferID string
}

func (c *CoordinatorStore) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	if sequenceNumber == "" {
		return xerrors.Errorf("sequence number should not be empty")
	}
	state := new(coordinator.TransferStateData)
	state.Generic = sequenceNumber
	return c.cp.SetTransferState(c.transferID, map[string]*coordinator.TransferStateData{
		streamName + ":" + shardID: state,
	})
}

func (c *CoordinatorStore) GetCheckpoint(streamName, shardID string) (string, error) {
	state, err := c.cp.GetTransferState(c.transferID)
	if err != nil {
		return "", xerrors.Errorf("unabe to load state: %w", err)
	}
	val, ok := state[streamName+":"+shardID]
	if !ok {
		return "", nil
	}
	return val.GetGeneric().(string), nil
}
