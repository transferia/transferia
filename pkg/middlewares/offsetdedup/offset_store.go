package offsetdedup

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
)

// Position is a unique watermark within a partition.
// Offset alone is not enough: one queue message (or WAL record) may
// expand into several ChangeItems that share Offset and differ by Index.
type Position struct {
	Offset uint64
	Index  int
}

// OffsetStore provides the last committed position from sink's storage.
// The table is supplied lazily from the first ChangeItem in the stream. One
// OffsetDedup instance serves exactly one table in one partition.
// Read-only — the sink is responsible for persisting positions.
// Returns nil if no position was saved (fresh start).
type OffsetStore interface {
	LoadLastPosition(ctx context.Context, partition abstract.Partition, tableID abstract.TableID) (*Position, error)
}

// OffsetStoreBuilder is an optional capability of QueueToS3Sink
// implementations that persist a restart watermark (e.g. Iceberg AsyncSink).
type OffsetStoreBuilder interface {
	BuildOffsetStore(transferID string) OffsetStore
}
