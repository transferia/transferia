//go:build !disable_postgres_provider

package splitter

import "context"

func BuildSplitter(ctx context.Context, storage postgresStorage, desiredTableSize uint64, snapshotDegreeOfParallelism int, isView, hasDataFiltration bool) Splitter {
	if isView {
		return NewView(storage, snapshotDegreeOfParallelism)
	} else {
		if hasDataFiltration {
			return NewTableIncrement(storage, desiredTableSize, snapshotDegreeOfParallelism)
		} else {
			return NewTableFull(storage, desiredTableSize, snapshotDegreeOfParallelism)
		}
	}
}
