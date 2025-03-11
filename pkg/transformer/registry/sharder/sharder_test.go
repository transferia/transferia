package sharder

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestSharderTransformer(t *testing.T) {
	t.Parallel()
	tf1, _ := filter.NewFilter(nil, []string{})
	cf1, _ := filter.NewFilter([]string{}, nil)
	allToStringTransformer := SharderTransformer{
		Tables:    tf1,
		Columns:   cf1,
		ShardsNum: 2,
		Logger:    logger.Log,
	}
	tf2, _ := filter.NewFilter([]string{"db.table"}, nil)
	cf2, _ := filter.NewFilter([]string{}, []string{"column2"})
	excludeColTransformer := SharderTransformer{
		Tables:    tf2,
		Columns:   cf2,
		ShardsNum: 4,
		Logger:    logger.Log,
	}
	tf3, _ := filter.NewFilter([]string{"db.a_table3"}, nil)
	cf3, _ := filter.NewFilter([]string{"column1", "column3"}, nil)
	includeTwoColsTransformer := SharderTransformer{
		Tables:    tf3,
		Columns:   cf3,
		ShardsNum: 8,
		Logger:    logger.Log,
	}

	table1 := abstract.TableID{
		Namespace: "db",
		Name:      "table1",
	}

	table2 := abstract.TableID{
		Namespace: "db",
		Name:      "table2",
	}

	table3 := abstract.TableID{
		Namespace: "db",
		Name:      "a_table3",
	}

	table1Schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column1", string(schema.TypeString), true),
		abstract.MakeTypedColSchema("column2", string(schema.TypeInt64), false),
		abstract.MakeTypedColSchema("column3", string(schema.TypeInt32), false),
		abstract.MakeTypedColSchema("column4", string(schema.TypeBoolean), false),
	})

	table2Schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column1", string(schema.TypeBytes), false),
		abstract.MakeTypedColSchema("column2", string(schema.TypeDate), false),
		abstract.MakeTypedColSchema("column3", string(schema.TypeFloat64), false),
		abstract.MakeTypedColSchema("column4", string(schema.TypeFloat32), false),
	})

	table3Schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column2", string(schema.TypeInt8), false),
		abstract.MakeTypedColSchema("column3", string(schema.TypeUint32), false),
		abstract.MakeTypedColSchema("column4", string(schema.TypeDate), false),
	})

	require.True(t, allToStringTransformer.Suitable(table1, table1Schema))
	require.True(t, allToStringTransformer.Suitable(table2, table2Schema))
	require.True(t, allToStringTransformer.Suitable(table3, table3Schema))

	require.True(t, excludeColTransformer.Suitable(table1, table1Schema))
	require.True(t, excludeColTransformer.Suitable(table2, table2Schema))
	require.False(t, excludeColTransformer.Suitable(table3, table3Schema))

	require.False(t, includeTwoColsTransformer.Suitable(table1, table1Schema))
	require.False(t, includeTwoColsTransformer.Suitable(table2, table2Schema))
	require.True(t, includeTwoColsTransformer.Suitable(table3, table3Schema))

	item1 := abstract.ChangeItem{
		Kind:         "Insert",
		Schema:       "db",
		Table:        "table1",
		ColumnNames:  []string{"column1", "column2", "column3", "column4"},
		ColumnValues: []interface{}{"value1", int64(123), int32(1234), true},
		TableSchema:  table1Schema,
	}

	item2 := abstract.ChangeItem{
		Kind:        "Insert",
		Schema:      "db",
		Table:       "table2",
		ColumnNames: []string{"colunm1", "column2", "column3", "column4"},
		ColumnValues: []interface{}{
			"value1",
			time.Date(1703, 1, 2, 0, 0, 0, 0, time.UTC),
			123.123,
			float32(312.321)},
		TableSchema: table2Schema,
	}
	item3 := abstract.ChangeItem{
		Kind:         "Insert",
		Schema:       "db",
		Table:        "a_table3",
		ColumnNames:  []string{"column2", "column3", "column4"},
		ColumnValues: []interface{}{int8(-3), uint32(12345), time.Minute},
		TableSchema:  table3Schema,
	}
	var result []abstract.TransformerResult
	for _, transformer := range []SharderTransformer{allToStringTransformer, excludeColTransformer, includeTwoColsTransformer} {
		for _, item := range []abstract.ChangeItem{item1, item2, item3} {
			if transformer.Suitable(item.TableID(), item.TableSchema) {
				result = append(result, transformer.Apply([]abstract.ChangeItem{item}))
			}
		}
	}

	canon.SaveJSON(t, result)
}
