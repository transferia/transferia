package engines

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestBuildDDLForHomoSink(t *testing.T) {
	t.Run("TM-8875 - when MergeTree becomes ReplicatedMergeTree - it shouldn't have parameters", func(t *testing.T) {
		in := "CREATE TABLE logs.test7 (`id` String, `counter` Int32) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8192"
		out, err := BuildDDLForHomoSink(
			in,
			true,
			"my_cluster_name",
			"dst_db",
			nil,
			abstract.TableID{
				Namespace: "namespace",
				Name:      "name",
			},
		)
		require.NoError(t, err)
		require.Equal(t, "CREATE TABLE IF NOT EXISTS logs.test7  ON CLUSTER `my_cluster_name` (`id` String, `counter` Int32) ENGINE = ReplicatedMergeTree() ORDER BY id SETTINGS index_granularity = 8192", out)
	})

	t.Run("TM-8776 - when ReplicatedMergeTree becomes MergeTree", func(t *testing.T) {
		in := "CREATE TABLE logs.test7 (`id` String, `counter` Int32) ENGINE = ReplicatedMergeTree() ORDER BY id SETTINGS index_granularity = 8192"
		out, err := BuildDDLForHomoSink(
			in,
			false,
			"my_cluster_name",
			"dst_db",
			nil,
			abstract.TableID{
				Namespace: "namespace",
				Name:      "name",
			},
		)
		require.NoError(t, err)
		require.Equal(t, "CREATE TABLE IF NOT EXISTS logs.test7 (`id` String, `counter` Int32) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8192", out)
	})
}
