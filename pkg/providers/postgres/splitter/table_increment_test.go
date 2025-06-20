//go:build !disable_postgres_provider

package splitter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

type checkConfigTableIncrement struct {
	wholeTableSizeInBytes             uint64
	wholeTableRowsCount               uint64
	tableDescriptionRowsCount         uint64
	desiredSize                       uint64
	snapshotDegreeOfParallelism       uint64
	expectedDolivochkaDataSizeInBytes uint64
	expectedDolivochkaDataSizeInRows  uint64
	expectedPartsCount                uint64
	hasReason                         bool
}

func checkTableIncrement(t *testing.T, config checkConfigTableIncrement) {
	postgresStorage := &postgresStorageStub{
		WholeTableSizeInBytes_:    config.wholeTableSizeInBytes,
		WholeTableRowsCount:       config.wholeTableRowsCount,
		TableDescriptionRowsCount: config.tableDescriptionRowsCount,
	}

	tableIncrement := NewTableIncrement(postgresStorage, config.desiredSize, int(config.snapshotDegreeOfParallelism))

	ctx := context.Background()
	splittedTableMetadata, err := tableIncrement.Split(ctx, abstract.TableDescription{})
	require.Equal(t, config.hasReason, abstract.IsNonShardableError(err))
	if splittedTableMetadata != nil {
		require.Equal(t, config.expectedDolivochkaDataSizeInBytes, splittedTableMetadata.DataSizeInBytes)
		require.Equal(t, config.expectedDolivochkaDataSizeInRows, splittedTableMetadata.DataSizeInRows)
		require.Equal(t, config.expectedPartsCount, splittedTableMetadata.PartsCount)
	}
}

func TestTableIncrement(t *testing.T) {
	//----------------------------------------------------------------------
	// NOT sharding

	t.Run("case: dolivochka is too small - then we are NOT sharding!", func(t *testing.T) {
		checkTableIncrement(t, checkConfigTableIncrement{
			wholeTableSizeInBytes:             100 * mb,
			wholeTableRowsCount:               1000, // #rows
			tableDescriptionRowsCount:         100,  // upload part of 10% table
			desiredSize:                       1 * gb,
			snapshotDegreeOfParallelism:       4,
			expectedDolivochkaDataSizeInBytes: 0,
			expectedDolivochkaDataSizeInRows:  0, // #rows
			expectedPartsCount:                0,
			hasReason:                         true,
		})
	})

	t.Run("case: dolivochka is too small - then we are NOT sharding!", func(t *testing.T) {
		checkTableIncrement(t, checkConfigTableIncrement{
			wholeTableSizeInBytes:             1 * gb,
			wholeTableRowsCount:               1000, // #rows
			tableDescriptionRowsCount:         100,  // upload part of 10% table
			desiredSize:                       1 * gb,
			snapshotDegreeOfParallelism:       4,
			expectedDolivochkaDataSizeInBytes: 0,
			expectedDolivochkaDataSizeInRows:  0, // #rows
			expectedPartsCount:                0,
			hasReason:                         true,
		})
	})

	t.Run("case: if somewhy tableSize is zero, we think table is really zero size, and it's too small - then we are NOT sharding!", func(t *testing.T) {
		checkTableIncrement(t, checkConfigTableIncrement{
			wholeTableSizeInBytes:             0,    // <-------------------------- wholeTableSizeInBytes=0
			wholeTableRowsCount:               1000, // #rows
			tableDescriptionRowsCount:         100,  // upload part of 10% table
			desiredSize:                       1 * gb,
			snapshotDegreeOfParallelism:       4,
			expectedDolivochkaDataSizeInBytes: 0,
			expectedDolivochkaDataSizeInRows:  0, // #rows
			expectedPartsCount:                0,
			hasReason:                         true,
		})
	})

	t.Run("case: if somewhy tableSize is zero & something else - we think table is really zero size - then we are NOT sharding!", func(t *testing.T) {
		checkTableIncrement(t, checkConfigTableIncrement{
			wholeTableSizeInBytes:             0,   // <-------------------------- wholeTableSizeInBytes=0
			wholeTableRowsCount:               0,   // #rows <-------------------- wholeTableRowsCount=0
			tableDescriptionRowsCount:         100, // upload part of 10% table
			desiredSize:                       1 * gb,
			snapshotDegreeOfParallelism:       4,
			expectedDolivochkaDataSizeInBytes: 0,
			expectedDolivochkaDataSizeInRows:  0, // #rows
			expectedPartsCount:                0,
			hasReason:                         true,
		})
	})

	t.Run("case: if somewhy tableSize is zero & something else - we think table is really zero size - then we are NOT sharding!", func(t *testing.T) {
		checkTableIncrement(t, checkConfigTableIncrement{
			wholeTableSizeInBytes:             0, // <-------------------------- wholeTableSizeInBytes=0
			wholeTableRowsCount:               0, // #rows <-------------------- wholeTableRowsCount=0
			tableDescriptionRowsCount:         0, // <-------------------------- tableDescriptionRowsCount=0 means TIMEOUT
			desiredSize:                       1 * gb,
			snapshotDegreeOfParallelism:       4,
			expectedDolivochkaDataSizeInBytes: 0,
			expectedDolivochkaDataSizeInRows:  0, // #rows
			expectedPartsCount:                0,
			hasReason:                         true,
		})
	})

	//----------------------------------------------------------------------
	// sharding

	t.Run("case: upload new 2gb piece, from 20gb table - then we are sharding!", func(t *testing.T) {
		checkTableIncrement(t, checkConfigTableIncrement{
			wholeTableSizeInBytes:             20 * gb,
			wholeTableRowsCount:               1000, // #rows
			tableDescriptionRowsCount:         100,  // upload part of 10% table
			desiredSize:                       1 * gb,
			snapshotDegreeOfParallelism:       4,
			expectedDolivochkaDataSizeInBytes: 2147483600, // ~2gb
			expectedDolivochkaDataSizeInRows:  100,        // #rows
			expectedPartsCount:                4,
			hasReason:                         false,
		})
	})

	t.Run("case: calculating exact count exceed 15seconds - then we are sharding!", func(t *testing.T) {
		checkTableIncrement(t, checkConfigTableIncrement{
			wholeTableSizeInBytes:             10 * gb,
			wholeTableRowsCount:               1000, // #rows
			tableDescriptionRowsCount:         0,    //  <-------------------------- tableDescriptionRowsCount=0 means TIMEOUT
			desiredSize:                       1 * gb,
			snapshotDegreeOfParallelism:       4,
			expectedDolivochkaDataSizeInBytes: 0,
			expectedDolivochkaDataSizeInRows:  0, // #rows
			expectedPartsCount:                4,
			hasReason:                         false,
		})
	})

	t.Run("case: if somewhy estimating of wholeTableRowsCount is zero - then we can't estimate DolivochkaDataSizeInBytes, but sharding!", func(t *testing.T) {
		checkTableIncrement(t, checkConfigTableIncrement{
			wholeTableSizeInBytes:             20 * gb,
			wholeTableRowsCount:               0,   // #rows <-------------------------- wholeTableRowsCount=0
			tableDescriptionRowsCount:         100, // upload part of 10% table
			desiredSize:                       1 * gb,
			snapshotDegreeOfParallelism:       4,
			expectedDolivochkaDataSizeInBytes: 0,
			expectedDolivochkaDataSizeInRows:  100, // #rows
			expectedPartsCount:                4,
			hasReason:                         false,
		})
	})

	t.Run("case: if somewhy estimating of wholeTableRowsCount is zero & calculating exact #rows took >15sec - then we can't estimate DolivochkaDataSizeInBytes, but sharding!", func(t *testing.T) {
		checkTableIncrement(t, checkConfigTableIncrement{
			wholeTableSizeInBytes:             20 * gb,
			wholeTableRowsCount:               0, // #rows <-------------------------- wholeTableRowsCount=0
			tableDescriptionRowsCount:         0, // <-------------------------------- tableDescriptionRowsCount=0 means TIMEOUT
			desiredSize:                       1 * gb,
			snapshotDegreeOfParallelism:       4,
			expectedDolivochkaDataSizeInBytes: 0,
			expectedDolivochkaDataSizeInRows:  0, // #rows
			expectedPartsCount:                4,
			hasReason:                         false,
		})
	})
}
