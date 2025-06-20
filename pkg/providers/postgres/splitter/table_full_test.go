//go:build !disable_postgres_provider

package splitter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

type checkConfigTableFull struct {
	wholeTableSizeInBytes             uint64
	wholeTableRowsCount               uint64
	desiredSize                       uint64
	snapshotDegreeOfParallelism       uint64
	expectedWholeTableDataSizeInBytes uint64
	expectedWholeTableDataSizeInRows  uint64
	expectedPartsCount                uint64
	hasReason                         bool
}

func checkTableFull(t *testing.T, config checkConfigTableFull) {
	postgresStorage := &postgresStorageStub{
		WholeTableSizeInBytes_:    config.wholeTableSizeInBytes,
		WholeTableRowsCount:       config.wholeTableRowsCount,
		TableDescriptionRowsCount: config.wholeTableRowsCount,
	}

	tableFull := NewTableFull(postgresStorage, config.desiredSize, int(config.snapshotDegreeOfParallelism))

	ctx := context.Background()
	splittedTableMetadata, err := tableFull.Split(ctx, abstract.TableDescription{})
	require.Equal(t, config.hasReason, abstract.IsNonShardableError(err))
	if splittedTableMetadata != nil {
		require.Equal(t, config.expectedWholeTableDataSizeInBytes, splittedTableMetadata.DataSizeInBytes)
		require.Equal(t, config.expectedWholeTableDataSizeInRows, splittedTableMetadata.DataSizeInRows)
		require.Equal(t, config.expectedPartsCount, splittedTableMetadata.PartsCount)
	}
}

func TestTableFull(t *testing.T) {
	checkTableFull(t, checkConfigTableFull{
		wholeTableSizeInBytes:             100 * mb,
		wholeTableRowsCount:               1000, // #rows
		desiredSize:                       1 * gb,
		snapshotDegreeOfParallelism:       4,
		expectedWholeTableDataSizeInBytes: 0,
		expectedWholeTableDataSizeInRows:  0, // #rows
		expectedPartsCount:                0,
		hasReason:                         true,
	})

	checkTableFull(t, checkConfigTableFull{
		wholeTableSizeInBytes:             1 * gb,
		wholeTableRowsCount:               1000, // #rows
		desiredSize:                       1 * gb,
		snapshotDegreeOfParallelism:       4,
		expectedWholeTableDataSizeInBytes: 0,
		expectedWholeTableDataSizeInRows:  0, // #rows
		expectedPartsCount:                0,
		hasReason:                         true,
	})

	checkTableFull(t, checkConfigTableFull{
		wholeTableSizeInBytes:             1*gb + 1,
		wholeTableRowsCount:               1000, // #rows
		desiredSize:                       1 * gb,
		snapshotDegreeOfParallelism:       4,
		expectedWholeTableDataSizeInBytes: 1*gb + 1,
		expectedWholeTableDataSizeInRows:  1000, // #rows
		expectedPartsCount:                2,
		hasReason:                         false,
	})

	checkTableFull(t, checkConfigTableFull{
		wholeTableSizeInBytes:             10 * gb,
		wholeTableRowsCount:               1000, // #rows
		desiredSize:                       1 * gb,
		snapshotDegreeOfParallelism:       4,
		expectedWholeTableDataSizeInBytes: 10 * gb,
		expectedWholeTableDataSizeInRows:  1000, // #rows
		expectedPartsCount:                4,
		hasReason:                         false,
	})
}
