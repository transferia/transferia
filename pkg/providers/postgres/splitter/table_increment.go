//go:build !disable_postgres_provider

package splitter

import (
	"context"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/format"
	"go.ytsaurus.tech/library/go/core/log"
)

type TableIncrement struct {
	storage                     postgresStorage
	desiredTableSize            uint64
	snapshotDegreeOfParallelism int
}

func (t *TableIncrement) Split(ctx context.Context, table abstract.TableDescription) (*SplittedTableMetadata, error) {
	// whole table info

	tableFull := NewTableFull(t.storage, t.desiredTableSize, t.snapshotDegreeOfParallelism)
	wholeTableMetadata, err := tableFull.Split(ctx, table)
	if err != nil {
		return nil, xerrors.Errorf("unable to get full table info, table:%s, err: %w", table.Fqtn(), err)
	}

	// concrete 'dolivochka' info

	logger.Log.Info("Will calculate exact table rows count", log.String("table", table.String()))
	exactRowsCount, err := t.storage.ExactTableDescriptionRowsCount(ctx, table, 15*time.Second)
	if err != nil {
		return nil, xerrors.Errorf("failed to calculate table rows count, table:%s, err: %w", table.Fqtn(), err)
	}
	logger.Log.Infof("Got exact rows count for table %v: %v", table.Fqtn(), exactRowsCount)

	estimateBytes := uint64(0)
	if wholeTableMetadata.DataSizeInRows != 0 && exactRowsCount != 0 {
		estimateBytes = (wholeTableMetadata.DataSizeInBytes / wholeTableMetadata.DataSizeInRows) * exactRowsCount
	}

	// check if table too small for sharding

	if estimateBytes != 0 {
		if estimateBytes <= t.desiredTableSize { // smaller than 1 gb
			return nil, abstract.NewNonShardableError(xerrors.Errorf("Table %s increment size (%s) smaller than desired (%s), load as single shard", table.Fqtn(), format.SizeUInt64(estimateBytes), format.SizeUInt64(t.desiredTableSize)))
		}
	}

	return &SplittedTableMetadata{
		DataSizeInBytes: estimateBytes,
		DataSizeInRows:  exactRowsCount,
		PartsCount:      uint64(t.snapshotDegreeOfParallelism),
	}, nil
}

func NewTableIncrement(storage postgresStorage, desiredTableSize uint64, snapshotDegreeOfParallelism int) *TableIncrement {
	return &TableIncrement{
		storage:                     storage,
		desiredTableSize:            desiredTableSize,
		snapshotDegreeOfParallelism: snapshotDegreeOfParallelism,
	}
}
