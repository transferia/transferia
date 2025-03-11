package packer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func getTestChangeItem() *abstract.ChangeItem {
	return &abstract.ChangeItem{
		Schema: "my_schema",
		Table:  "my_table",
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "column_a", DataType: ytschema.TypeInt32.String(), OriginalType: "pg:int", PrimaryKey: true},
			{ColumnName: "column_b", DataType: ytschema.TypeString.String(), OriginalType: "pg:text", PrimaryKey: false},
		}),
	}
}

func TestTableSchemaKey(t *testing.T) {
	changeItem := getTestChangeItem()
	require.Equal(t, `"my_schema"."my_table"|column_a:int32:pg:int:true|column_b:utf8:pg:text:false|`, tableSchemaKey(changeItem))
}
