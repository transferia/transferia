package mask

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestHmacHasherTransformer(t *testing.T) {
	tf, _ := filter.NewFilter(nil, []string{})
	hmacHasherTransformer := HmacHasher{
		Tables:      tf,
		Columns:     set.New[string]("column1", "column2", "column3", "column4"),
		HashFactory: sha256.New,
		Salt:        "the-best-tasty-saint-petersburg-salt",
		lgr:         logger.Log,
	}

	table1 := *abstract.NewTableID("db", "table1")
	table2 := *abstract.NewTableID("db", "table2")
	table3 := *abstract.NewTableID("db", "table3")

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
	table2Schema.Columns()[0].OriginalType = "mysql:blob"

	table3Schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column2", string(schema.TypeInt8), false),
		abstract.MakeTypedColSchema("column3", string(schema.TypeUint32), false),
		abstract.MakeTypedColSchema("column4", string(schema.TypeDate), false),
	})

	require.True(t, hmacHasherTransformer.Suitable(table1, table1Schema))
	require.True(t, hmacHasherTransformer.Suitable(table2, table2Schema))
	require.True(t, hmacHasherTransformer.Suitable(table3, table3Schema))

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
		ColumnNames: []string{"column1", "column2", "column3", "column4"},
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
	item4 := abstract.ChangeItem{
		Kind:         "Insert",
		Schema:       "db",
		Table:        "a_table3",
		ColumnNames:  []string{},
		ColumnValues: []interface{}{},
		TableSchema:  table3Schema,
	}
	var result []abstract.TransformerResult
	for _, item := range []abstract.ChangeItem{item1, item2, item3, item4} {
		if hmacHasherTransformer.Suitable(item.TableID(), item.TableSchema) {
			result = append(result, hmacHasherTransformer.Apply([]abstract.ChangeItem{item}))
		}
	}

	canon.SaveJSON(t, result)
}
