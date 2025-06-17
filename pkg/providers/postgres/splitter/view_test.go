//go:build !disable_postgres_provider

package splitter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

type checkConfigView struct {
	wholeViewRowsCount          uint64
	snapshotDegreeOfParallelism int
	expectedViewDataSizeInRows  uint64
	expectedPartsCount          uint64
}

func checkView(t *testing.T, config checkConfigView) {
	postgresStorage := &postgresStorageStub{
		WholeTableSizeInBytes_:    0,
		WholeTableRowsCount:       config.wholeViewRowsCount,
		TableDescriptionRowsCount: config.wholeViewRowsCount,
	}

	view := NewView(postgresStorage, config.snapshotDegreeOfParallelism)

	ctx := context.Background()
	splittedTableMetadata, err := view.Split(ctx, abstract.TableDescription{})
	require.NoError(t, err)
	if splittedTableMetadata != nil {
		require.Equal(t, uint64(0), splittedTableMetadata.DataSizeInBytes)
		require.Equal(t, config.expectedViewDataSizeInRows, splittedTableMetadata.DataSizeInRows)
		require.Equal(t, config.expectedPartsCount, splittedTableMetadata.PartsCount)
	}
}

func TestView(t *testing.T) {
	t.Run("case: views always will be sharded", func(t *testing.T) {
		checkView(t, checkConfigView{
			wholeViewRowsCount:          1000, // #rows
			snapshotDegreeOfParallelism: 4,
			expectedViewDataSizeInRows:  1000, // #rows
			expectedPartsCount:          4,
		})
	})

	t.Run("case: views always will be sharded - and when timeout", func(t *testing.T) {
		checkView(t, checkConfigView{
			wholeViewRowsCount:          0, // #rows <------------------- wholeViewRowsCount=0 means TIMEOUT
			snapshotDegreeOfParallelism: 4,
			expectedViewDataSizeInRows:  0, // #rows
			expectedPartsCount:          4,
		})
	})
}
