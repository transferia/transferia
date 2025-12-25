package helpers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestLoadTable(t *testing.T) {
	tableSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{
			TableSchema: "schema",
			TableName:   "table",
			ColumnName:  "a",
			DataType:    ytschema.TypeString.String(),
			PrimaryKey:  true,
		},
		{
			TableSchema: "schema",
			TableName:   "table",
			ColumnName:  "b",
			DataType:    ytschema.TypeInt64.String(),
			PrimaryKey:  true,
		},
		{
			TableSchema: "schema",
			TableName:   "table",
			ColumnName:  "c",
			DataType:    ytschema.TypeInt64.String(),
			PrimaryKey:  false,
		},
	})

	storage := NewFakeStorage([]abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{"val_b", 3},
			TableSchema:  tableSchema,
		},
		{
			Kind:         abstract.InsertKind,
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{"val_a", 2},
			TableSchema:  tableSchema,
		},
		{
			Kind:         abstract.InsertKind,
			ColumnNames:  []string{"a", "b"},
			ColumnValues: []interface{}{"val_a", 1},
			TableSchema:  tableSchema,
		},
	})
	changeItems := LoadTable(t, storage, abstract.TableDescription{})
	require.Equal(t, []interface{}{"val_a", 1}, changeItems[0].ColumnValues)
	require.Equal(t, []interface{}{"val_a", 2}, changeItems[1].ColumnValues)
	require.Equal(t, []interface{}{"val_b", 3}, changeItems[2].ColumnValues)
}
