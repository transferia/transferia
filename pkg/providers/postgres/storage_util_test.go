package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestHandlePartitionedTables(t *testing.T) {
	t.Run("remove partitioned table", func(t *testing.T) {
		foundTables := map[abstract.TableID]abstract.TableInfo{
			{Namespace: "", Name: "table0"}: {},
			{Namespace: "", Name: "table1"}: {},
			{Namespace: "", Name: "tableA"}: {},
		}

		childToParent := map[abstract.TableID]abstract.TableID{
			{Namespace: "", Name: "table0"}: {Namespace: "", Name: "tableA"},
			{Namespace: "", Name: "table1"}: {Namespace: "", Name: "tableA"},
		}

		tableMap, err := handlePartitionedTables(logger.Log, foundTables, childToParent)
		require.NoError(t, err)
		require.Equal(t, 1, len(tableMap))
	})

	t.Run("error, when matched partitioned_table and not all it's parts", func(t *testing.T) {
		foundTables := map[abstract.TableID]abstract.TableInfo{
			{Namespace: "", Name: "table1"}: {},
			{Namespace: "", Name: "tableA"}: {},
		}

		childToParent := map[abstract.TableID]abstract.TableID{
			{Namespace: "", Name: "table0"}: {Namespace: "", Name: "tableA"},
			{Namespace: "", Name: "table1"}: {Namespace: "", Name: "tableA"},
		}

		_, err := handlePartitionedTables(logger.Log, foundTables, childToParent)
		require.Error(t, err)
	})

	t.Run("remove partitioned table", func(t *testing.T) {
		foundTables := map[abstract.TableID]abstract.TableInfo{
			{Namespace: "", Name: "table0"}: {},
			{Namespace: "", Name: "table1"}: {},
		}

		childToParent := map[abstract.TableID]abstract.TableID{
			{Namespace: "", Name: "table0"}: {Namespace: "", Name: "tableA"},
			{Namespace: "", Name: "table1"}: {Namespace: "", Name: "tableA"},
		}

		tableMap, err := handlePartitionedTables(logger.Log, foundTables, childToParent)
		require.NoError(t, err)
		require.Equal(t, 2, len(tableMap))
	})
}
