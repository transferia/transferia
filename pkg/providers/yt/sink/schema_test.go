//go:build !disable_yt_provider

package sink

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/yt"
)

func TestSystemKeysWorkaround(t *testing.T) {
	config := yt.NewYtDestinationV1(yt.YtDestination{
		Path: "//home/cdc/test/ok/TM-5580-sorted",
	})

	t.Run("reorder", func(t *testing.T) {
		schema := NewSchema(
			[]abstract.ColSchema{
				{ColumnName: "key", PrimaryKey: true},
				{ColumnName: "_timestamp", PrimaryKey: true},
				{ColumnName: "_partition", PrimaryKey: true},
				{ColumnName: "_offset", PrimaryKey: true},
				{ColumnName: "_idx", PrimaryKey: true},
				{ColumnName: "val", PrimaryKey: false},
			},
			config,
			"",
		)
		cols := schema.Cols()
		require.Equal(t, "_timestamp", cols[0].ColumnName)
		require.Equal(t, "key", cols[4].ColumnName)
		require.Equal(t, "val", cols[5].ColumnName)
	})

	t.Run("no reorder", func(t *testing.T) {
		schema := NewSchema(
			[]abstract.ColSchema{
				{ColumnName: "key", PrimaryKey: true},
				{ColumnName: "_timestamp", PrimaryKey: false},
				{ColumnName: "_partition", PrimaryKey: false},
				{ColumnName: "_offset", PrimaryKey: false},
				{ColumnName: "_idx", PrimaryKey: false},
				{ColumnName: "val", PrimaryKey: false},
			},
			config,
			"",
		)
		cols := schema.Cols()
		require.Equal(t, "key", cols[0].ColumnName)
		require.Equal(t, "_timestamp", cols[1].ColumnName)
		require.Equal(t, "val", cols[5].ColumnName)
	})
}
