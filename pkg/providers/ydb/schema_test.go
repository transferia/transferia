//go:build !disable_ydb_provider

package ydb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func TestFromYdbSchema(t *testing.T) {
	t.Run("direct order", func(t *testing.T) {
		resultColumns := FromYdbSchema([]options.Column{{Name: "a"}, {Name: "b"}}, []string{"a", "b"})
		require.Equal(t, 2, len(resultColumns))
		require.Equal(t, "a", resultColumns[0].ColumnName)
		require.Equal(t, "b", resultColumns[1].ColumnName)
	})
	t.Run("reverse order", func(t *testing.T) {
		resultColumns := FromYdbSchema([]options.Column{{Name: "b"}, {Name: "a"}}, []string{"a", "b"})
		require.Equal(t, 2, len(resultColumns))
		require.Equal(t, "a", resultColumns[0].ColumnName)
		require.Equal(t, "b", resultColumns[1].ColumnName)
	})
}
