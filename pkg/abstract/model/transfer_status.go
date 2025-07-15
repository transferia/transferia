package model

import "github.com/transferia/transferia/library/go/core/xerrors"

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

	// tmp statuses for async transfers
	Paused    = TransferStatus("Paused")    // replication is paused manually
	Preparing = TransferStatus("Preparing") // replication is being started
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
	Preparing:    true,

	Paused: false,
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
