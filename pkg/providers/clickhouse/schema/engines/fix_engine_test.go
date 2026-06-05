package engines

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixEngine(t *testing.T) {
	t.Run("check all conversion pairs", func(t *testing.T) {
		t.Run("MergeTree -> MergeTree", func(t *testing.T) {
			result, err := FixEngine(`MergeTree()`, false, false, "db", "table", "zkPath")
			require.NoError(t, err)
			require.Equal(t, `MergeTree()`, result)
		})
		t.Run("ReplicatedMergeTree -> MergeTree", func(t *testing.T) {
			result, err := FixEngine(`ReplicatedMergeTree()`, false, false, "db", "table", "zkPath")
			require.NoError(t, err)
			require.Equal(t, `MergeTree()`, result)
		})
		t.Run("ReplicatedMergeTree -> MergeTree", func(t *testing.T) {
			result, err := FixEngine(`ReplicatedMergeTree('a/b', '{replica}')`, false, false, "db", "table", "zkPath")
			require.NoError(t, err)
			require.Equal(t, `MergeTree()`, result)
		})
		t.Run("MergeTree -> ReplicatedMergeTree", func(t *testing.T) {
			result, err := FixEngine(`MergeTree()`, true, false, "db", "table", "zkPath")
			require.NoError(t, err)
			require.Equal(t, `ReplicatedMergeTree()`, result)
		})
	})

	t.Run("check all parameters after engine - saved", func(t *testing.T) {
		t.Run("MergeTree -> MergeTree", func(t *testing.T) {
			result, err := FixEngine(`MergeTree() ORDER BY id SETTINGS index_granularity = 8192`, false, false, "db", "table", "myZkPath")
			require.NoError(t, err)
			require.Equal(t, `MergeTree() ORDER BY id SETTINGS index_granularity = 8192`, result)
		})
		t.Run("MergeTree -> ReplicatedMergeTree", func(t *testing.T) {
			result, err := FixEngine(`MergeTree() ORDER BY id SETTINGS index_granularity = 8192`, true, false, "db", "table", "myZkPath")
			require.NoError(t, err)
			require.Equal(t, `ReplicatedMergeTree() ORDER BY id SETTINGS index_granularity = 8192`, result)
		})
		t.Run("ReplicatedMergeTree -> MergeTree", func(t *testing.T) {
			result, err := FixEngine(`ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}') ORDER BY id SETTINGS index_granularity = 8192`, false, false, "db", "table", "myZkPath")
			require.NoError(t, err)
			require.Equal(t, `MergeTree() ORDER BY id SETTINGS index_granularity = 8192`, result)
		})
		t.Run("ReplicatedMergeTree -> ReplicatedMergeTree", func(t *testing.T) {
			result, err := FixEngine(`ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}') ORDER BY id SETTINGS index_granularity = 8192`, true, false, "db", "table", "myZkPath")
			require.NoError(t, err)
			require.Equal(t, `ReplicatedMergeTree(myZkPath, '{replica}') ORDER BY id SETTINGS index_granularity = 8192`, result)
		})
	})

	t.Run("Replicated database - explicit zookeeper_path/replica_name must be omitted", func(t *testing.T) {
		t.Run("MergeTree -> ReplicatedMergeTree", func(t *testing.T) {
			result, err := FixEngine(`MergeTree() ORDER BY id`, true, true, "db", "table", "myZkPath")
			require.NoError(t, err)
			require.Equal(t, `ReplicatedMergeTree() ORDER BY id`, result)
		})
		t.Run("ReplacingMergeTree -> ReplicatedReplacingMergeTree keeps base params", func(t *testing.T) {
			result, err := FixEngine(`ReplacingMergeTree(ver) ORDER BY id`, true, true, "db", "table", "myZkPath")
			require.NoError(t, err)
			require.Equal(t, `ReplicatedReplacingMergeTree(ver) ORDER BY id`, result)
		})
		t.Run("ReplicatedReplacingMergeTree drops zk args, keeps base params", func(t *testing.T) {
			result, err := FixEngine(`ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}', ver) ORDER BY id`, true, true, "db", "table", "myZkPath")
			require.NoError(t, err)
			require.Equal(t, `ReplicatedReplacingMergeTree(ver) ORDER BY id`, result)
		})
		t.Run("already argless ReplicatedMergeTree stays unchanged", func(t *testing.T) {
			result, err := FixEngine(`ReplicatedMergeTree() ORDER BY id`, true, true, "db", "table", "myZkPath")
			require.NoError(t, err)
			require.Equal(t, `ReplicatedMergeTree() ORDER BY id`, result)
		})
	})
}
