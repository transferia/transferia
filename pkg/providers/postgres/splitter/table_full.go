//go:build !disable_postgres_provider

package splitter

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/format"
)

type TableFull struct {
	storage                     postgresStorage
	desiredTableSize            uint64
	snapshotDegreeOfParallelism int
}

func (t *TableFull) Split(ctx context.Context, table abstract.TableDescription) (*SplittedTableMetadata, error) {
	wholeTableSizeInBytes, err := t.storage.TableSizeInBytes(table.ID())
	if err != nil {
		return nil, xerrors.Errorf("unable to get table byte size: %w", err)
	}

	// check if table too small for sharding
	if wholeTableSizeInBytes <= t.desiredTableSize { // by default: smaller than 1 gb
		return nil, abstract.NewNonShardableError(xerrors.Errorf("Table %v size (%v) smaller than desired (%v), load as single shard", table.Fqtn(), format.SizeUInt64(wholeTableSizeInBytes), format.SizeUInt64(t.desiredTableSize)))
	}

	wholeTableEstimateRows, err := t.storage.EstimateTableRowsCount(table.ID())
	if err != nil {
		return nil, xerrors.Errorf("failed to define table %v rows count: %w", table.Fqtn(), err)
	}

	partCount := calculatePartCount(wholeTableSizeInBytes, t.desiredTableSize, uint64(t.snapshotDegreeOfParallelism))

	return &SplittedTableMetadata{
		DataSizeInBytes: wholeTableSizeInBytes,
		DataSizeInRows:  wholeTableEstimateRows,
		PartsCount:      partCount,
	}, nil
}

func NewTableFull(storage postgresStorage, desiredTableSize uint64, snapshotDegreeOfParallelism int) *TableFull {
	return &TableFull{
		storage:                     storage,
		desiredTableSize:            desiredTableSize,
		snapshotDegreeOfParallelism: snapshotDegreeOfParallelism,
	}
}
