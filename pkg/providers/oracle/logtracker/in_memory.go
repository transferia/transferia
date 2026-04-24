package logtracker

import (
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
)

type InMemoryLogTracker struct {
	transferID string
	position   *oracle_common.LogPosition
}

func NewInMemoryLogTracker(transferID string) (*InMemoryLogTracker, error) {
	tracker := &InMemoryLogTracker{
		transferID: transferID,
		position:   nil,
	}
	return tracker, nil
}

func (tracker *InMemoryLogTracker) TransferID() string {
	return tracker.transferID
}

func (tracker *InMemoryLogTracker) Init() error {
	return nil
}

func (tracker *InMemoryLogTracker) ClearPosition() error {
	tracker.position = nil
	return nil
}

func (tracker *InMemoryLogTracker) ReadPosition() (*oracle_common.LogPosition, error) {
	return tracker.position, nil
}

func (tracker *InMemoryLogTracker) WritePosition(position *oracle_common.LogPosition) error {
	tracker.position = position
	return nil
}
