package raw_to_table_common

import (
	"testing"

	"github.com/stretchr/testify/require"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestBuildTableSchemaAndColumnNames_JSONKey(t *testing.T) {
	cfg := &CommonConfig{
		IsKeyEnabled: true,
		KeyType:      JSON,
		ValueType:    Bytes,
	}

	tableSchema, columnNames := BuildTableSchemaAndColumnNames(cfg, false)
	require.Contains(t, columnNames, ColNameKey)

	found := false
	for _, column := range tableSchema.Columns() {
		if column.ColumnName == ColNameKey {
			found = true
			require.Equal(t, ytschema.TypeAny.String(), column.DataType)
		}
	}
	require.True(t, found)
}
