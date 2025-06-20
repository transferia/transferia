//go:build !disable_ydb_provider

package ydb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestTableSchemaWrapper(t *testing.T) {
	tableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "a"},
	})

	currTableSchemaWrapper := newTableSchemaObj()
	currTableSchemaWrapper.Set(tableSchema)

	require.True(t, currTableSchemaWrapper.IsAllColumnNamesKnown(&cdcEvent{
		Update: map[string]interface{}{"a": 1},
	}))

	require.False(t, currTableSchemaWrapper.IsAllColumnNamesKnown(&cdcEvent{
		Update: map[string]interface{}{"a": 1, "b": 2},
	}))
	require.False(t, currTableSchemaWrapper.IsAllColumnNamesKnown(&cdcEvent{
		Update: map[string]interface{}{"b": 1},
	}))

	require.True(t, currTableSchemaWrapper.IsAllColumnNamesKnown(&cdcEvent{
		NewImage: map[string]interface{}{"a": 1},
	}))

	require.False(t, currTableSchemaWrapper.IsAllColumnNamesKnown(&cdcEvent{
		NewImage: map[string]interface{}{"a": 1, "b": 2},
	}))
	require.False(t, currTableSchemaWrapper.IsAllColumnNamesKnown(&cdcEvent{
		NewImage: map[string]interface{}{"b": 1},
	}))
}
