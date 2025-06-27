package engines

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixEngine(t *testing.T) {
	t.Run("check all conversion pairs", func(t *testing.T) {
		t.Run("MergeTree -> MergeTree", func(t *testing.T) {
			result, err := FixEngine(`MergeTree()`, false, "db", "table", "zkPath")
			require.NoError(t, err)
			require.Equal(t, `MergeTree()`, result)
		})
		t.Run("ReplicatedMergeTree -> MergeTree", func(t *testing.T) {
			result, err := FixEngine(`ReplicatedMergeTree()`, false, "db", "table", "zkPath")
			require.NoError(t, err)
			require.Equal(t, `MergeTree()`, result)
		})
		t.Run("ReplicatedMergeTree -> MergeTree", func(t *testing.T) {
			result, err := FixEngine(`ReplicatedMergeTree('a/b', '{replica}')`, false, "db", "table", "zkPath")
			require.NoError(t, err)
			require.Equal(t, `MergeTree()`, result)
		})
		t.Run("MergeTree -> ReplicatedMergeTree", func(t *testing.T) {
			result, err := FixEngine(`MergeTree()`, true, "db", "table", "zkPath")
			require.NoError(t, err)
			require.Equal(t, `ReplicatedMergeTree()`, result)
		})
	})

	t.Run("check all parameters after engine - saved", func(t *testing.T) {
		t.Run("MergeTree -> MergeTree", func(t *testing.T) {
			result, err := FixEngine(`MergeTree() ORDER BY id SETTINGS index_granularity = 8192`, false, "db", "table", "myZkPath")
			require.NoError(t, err)
			require.Equal(t, `MergeTree() ORDER BY id SETTINGS index_granularity = 8192`, result)
		})
		t.Run("MergeTree -> ReplicatedMergeTree", func(t *testing.T) {
			result, err := FixEngine(`MergeTree() ORDER BY id SETTINGS index_granularity = 8192`, true, "db", "table", "myZkPath")
			require.NoError(t, err)
			require.Equal(t, `ReplicatedMergeTree() ORDER BY id SETTINGS index_granularity = 8192`, result)
		})
		t.Run("ReplicatedMergeTree -> MergeTree", func(t *testing.T) {
			result, err := FixEngine(`ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}') ORDER BY id SETTINGS index_granularity = 8192`, false, "db", "table", "myZkPath")
			require.NoError(t, err)
			require.Equal(t, `MergeTree() ORDER BY id SETTINGS index_granularity = 8192`, result)
		})
		t.Run("ReplicatedMergeTree -> ReplicatedMergeTree", func(t *testing.T) {
			result, err := FixEngine(`ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}') ORDER BY id SETTINGS index_granularity = 8192`, true, "db", "table", "myZkPath")
			require.NoError(t, err)
			require.Equal(t, `ReplicatedMergeTree(myZkPath, '{replica}') ORDER BY id SETTINGS index_granularity = 8192`, result)
		})
	})
}
