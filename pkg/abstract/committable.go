package abstract

import "context"

// Committable is a sinker that needs to be completed after snapshot.
type Committable interface {
	Sinker

	// Commit completes all snapshot writes made by this transfer's sinks.
	// The context belongs to the snapshot operation and is canceled when it stops.
	Commit(ctx context.Context) error
}
