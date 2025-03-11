package coordinator

import (
	"context"

	"github.com/transferria/transferria/pkg/abstract/model"
)

type TransferHeartbeat struct {
	RetryCount int
	LastError  string
}

// StatusMessageProvider set time-based status for particular transfer
type StatusMessageProvider interface {
	// OpenStatusMessage open new line of error status message
	// for example: add timeout error for SOURCE db category
	OpenStatusMessage(transferID string, category string, content *StatusMessage) error
	// CloseStatusMessagesForCategory close line of error status message for certain category
	// for example: close timeout error for SOURCE db (once connectivity reached)
	CloseStatusMessagesForCategory(transferID string, category string) error
	// CloseStatusMessagesForTransfer remove all error lines for all categories
	CloseStatusMessagesForTransfer(transferID string) error
}

// TransferStatus main coordinator interface used by transfer
// just to start / stop transfer and move it via transfer workflow
type TransferStatus interface {
	// SetStatus move transfer to certain status
	SetStatus(transferID string, status model.TransferStatus) error
	// FailReplication stop replication with error
	FailReplication(transferID string, err error) error
	// TransferHealth add heartbeat for replication instance
	TransferHealth(ctx context.Context, transferID string, health *TransferHeartbeat) error
}

// TransferState is to manage transfer state, simple K-V structure
type TransferState interface {
	// GetTransferState return known transfer state
	GetTransferState(id string) (map[string]*TransferStateData, error)
	// SetTransferState set certain keys to transfer state.
	// it will add keys to exists state
	SetTransferState(transferID string, state map[string]*TransferStateData) error
	// RemoveTransferState remove certain state keys from state
	RemoveTransferState(transferID string, state []string) error
}
