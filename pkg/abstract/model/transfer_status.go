package model

import "github.com/transferria/transferria/library/go/core/xerrors"

type TransferStatus string

const (
	Running = TransferStatus("Running")
	Stop    = TransferStatus("Stop")
	New     = TransferStatus("New")

	Scheduled = TransferStatus("Scheduled")
	Started   = TransferStatus("Started")
	Completed = TransferStatus("Completed")
	Failed    = TransferStatus("Failed")

	Stopping     = TransferStatus("Stopping")
	Creating     = TransferStatus("Creating")
	Deactivating = TransferStatus("Deactivating")
	Failing      = TransferStatus("Failing")
)

var statusActivityMap = map[TransferStatus]bool{
	New:       false,
	Stop:      false,
	Completed: false,
	Failed:    false,

	Creating:     true,
	Scheduled:    true,
	Running:      true,
	Started:      true,
	Deactivating: true,
	Failing:      true,
	Stopping:     true,
}

var ActiveStatuses []TransferStatus

func IsActiveStatus(status TransferStatus) (bool, error) {
	isActive, ok := statusActivityMap[status]
	if !ok {
		return false, xerrors.Errorf("Unknown status %v", status)
	}
	return isActive, nil
}

func init() {
	for status, isActive := range statusActivityMap {
		if isActive {
			ActiveStatuses = append(ActiveStatuses, status)
		}
	}
}
