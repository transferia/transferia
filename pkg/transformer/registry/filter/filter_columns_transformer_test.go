package filter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/set"
)

func TestFilterColumnsTransformer(t *testing.T) {
	tableF, err := NewFilter([]string{"db.table1", "db.table2"}, nil)
	require.NoError(t, err)
	colF, err := NewFilter([]string{}, []string{"column1"})
	require.NoError(t, err)
	transformer := NewCustomFilterColumnsTransformer(
		tableF,
		colF,
		logger.Log,
	)

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
		Name:      "table3",
	}

	table1Schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column1", "String", true),
		abstract.MakeTypedColSchema("column2", "String", false),
		abstract.MakeTypedColSchema("column3", "String", false),
		abstract.MakeTypedColSchema("column4", "String", false),
	})

	table2Schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column1", "String", false),
		abstract.MakeTypedColSchema("column2", "String", false),
		abstract.MakeTypedColSchema("column3", "String", false),
		abstract.MakeTypedColSchema("column4", "String", false),
	})

	transformedSchema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column2", "String", false),
		abstract.MakeTypedColSchema("column3", "String", false),
		abstract.MakeTypedColSchema("column4", "String", false),
	})

	require.False(t, transformer.Suitable(table1, table1Schema))
	require.True(t, transformer.Suitable(table2, table2Schema))
	require.False(t, transformer.Suitable(table3, table2Schema))

	res, _ := transformer.ResultSchema(table2Schema)
	require.Equal(t, res, transformedSchema)

	oldKeys := abstract.OldKeysType{
		KeyNames:  []string{},
		KeyTypes:  []string{},
		KeyValues: []interface{}{},
	}

	item1 := new(abstract.ChangeItem)
	item1.Kind = "Insert"
	item1.Schema = "db"
	item1.Table = "table2"
	item1.ColumnNames = []string{"column1", "column2", "column3", "column4"}
	item1.ColumnValues = []interface{}{"value1", "value2", "value3", "value4"}
	item1.TableSchema = table2Schema
	item1.OldKeys = oldKeys

	item2 := new(abstract.ChangeItem)
	item2.Kind = "Insert"
	item2.Schema = "db"
	item2.Table = "table2"
	item2.ColumnNames = []string{"column2", "column3", "column4"}
	item2.ColumnValues = []interface{}{"value2", "value3", "value4"}
	item2.TableSchema = transformedSchema
	item2.OldKeys = oldKeys

	item3 := new(abstract.ChangeItem)
	item3.Kind = "Insert"
	item3.Schema = "db"
	item3.Table = "table2"
	item3.ColumnNames = []string{"column1", "column2", "column3", "column4"}
	item3.ColumnValues = []interface{}{"value1", "value2", "value3", "value4"}
	item3.TableSchema = table1Schema
	item3.OldKeys = oldKeys

	checkResult := abstract.TransformerResult{
		Transformed: []abstract.ChangeItem{*item2, *item2},
		Errors: []abstract.TransformerError{
			{
				Input: *item3,
				Error: nil,
			},
		},
	}

	result := transformer.Apply([]abstract.ChangeItem{*item1, *item2, *item3})
	require.Equal(t, result.Transformed[0].ColumnNames, checkResult.Transformed[0].ColumnNames)
	require.Equal(t, result.Transformed[0].ColumnValues, checkResult.Transformed[0].ColumnValues)
	require.Equal(t, result.Transformed[0].TableSchema.Columns(), checkResult.Transformed[0].TableSchema.Columns())
	require.Equal(t, result.Transformed[1].ColumnNames, checkResult.Transformed[1].ColumnNames)
	require.Equal(t, result.Transformed[1].ColumnValues, checkResult.Transformed[1].ColumnValues)
	require.Equal(t, result.Transformed[1].TableSchema.Columns(), checkResult.Transformed[1].TableSchema.Columns())
	require.Equal(t, len(result.Errors), len(checkResult.Errors))
}

func BenchmarkColumnsFilter(b *testing.B) {
	tableSchema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column0", "String", true),
		abstract.MakeTypedColSchema("column1", "String", false),
		abstract.MakeTypedColSchema("column2", "String", false),
		abstract.MakeTypedColSchema("column3", "String", false),
		abstract.MakeTypedColSchema("column4", "String", false),
		abstract.MakeTypedColSchema("column5", "String", false),
		abstract.MakeTypedColSchema("column6", "String", false),
		abstract.MakeTypedColSchema("column7", "String", false),
		abstract.MakeTypedColSchema("column8", "String", false),
		abstract.MakeTypedColSchema("column9", "String", false),
		abstract.MakeTypedColSchema("column10", "String", false),
	})
	tableF, err := NewFilter([]string{"db.table1"}, nil)
	require.NoError(b, err)
	colF, err := NewFilter([]string{}, []string{"column1", "column3", "column5", "column7", "column9"})
	require.NoError(b, err)
	transformer := NewCustomFilterColumnsTransformer(
		tableF,
		colF,
		logger.Log,
	)
	item := new(abstract.ChangeItem)
	item.Kind = "Insert"
	item.Schema = "db"
	item.Table = "table1"
	item.ColumnNames = []string{"column0", "column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "column10"}
	item.ColumnValues = []interface{}{"value0", "value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10"}
	item.TableSchema = tableSchema

	itemSize := util.DeepSizeof(item.ColumnValues)

	b.Run("filter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = transformer.Apply([]abstract.ChangeItem{*item})
		}
		b.SetBytes(int64(itemSize) * int64(b.N))
		b.ReportAllocs()
	})
}

func TestCopyAndTrimOldKeys(t *testing.T) {
	tests := []struct {
		name             string
		oldKeys          *abstract.OldKeysType
		filteredColumns  *set.Set[string]
		expectedKeyNames []string
		expectedKeyTypes []string
		expectedValues   []interface{}
	}{
		{
			name: "Empty input",
			oldKeys: &abstract.OldKeysType{
				KeyNames:  []string{},
				KeyTypes:  []string{},
				KeyValues: []interface{}{},
			},
			filteredColumns:  set.New[string](),
			expectedKeyNames: []string{},
			expectedKeyTypes: []string{},
			expectedValues:   []interface{}{},
		},
		{
			name: "All keys filtered out",
			oldKeys: &abstract.OldKeysType{
				KeyNames:  []string{"key1", "key2"},
				KeyTypes:  []string{"string", "int"},
				KeyValues: []interface{}{"value1", 42},
			},
			filteredColumns:  set.New[string](),
			expectedKeyNames: []string{},
			expectedKeyTypes: []string{},
			expectedValues:   []interface{}{},
		},
		{
			name: "Some keys filtered",
			oldKeys: &abstract.OldKeysType{
				KeyNames:  []string{"key1", "key2", "key3"},
				KeyTypes:  []string{"string", "int", "bool"},
				KeyValues: []interface{}{"value1", 42, true},
			},
			filteredColumns:  set.New("key1", "key3"),
			expectedKeyNames: []string{"key1", "key3"},
			expectedKeyTypes: []string{"string", "bool"},
			expectedValues:   []interface{}{"value1", true},
		},
		{
			name: "All keys kept",
			oldKeys: &abstract.OldKeysType{
				KeyNames:  []string{"key1", "key2"},
				KeyTypes:  []string{"string", "int"},
				KeyValues: []interface{}{"value1", 42},
			},
			filteredColumns:  set.New("key1", "key2"),
			expectedKeyNames: []string{"key1", "key2"},
			expectedKeyTypes: []string{"string", "int"},
			expectedValues:   []interface{}{"value1", 42},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := copyAndTrimOldKeys(tt.oldKeys, *tt.filteredColumns)
			require.Equal(t, tt.expectedKeyNames, result.KeyNames)
			require.Equal(t, tt.expectedKeyTypes, result.KeyTypes)
			require.Equal(t, tt.expectedValues, result.KeyValues)
		})
	}
}
