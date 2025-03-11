package tablesplitter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/transformer/registry/filter"
	"go.ytsaurus.tech/yt/go/schema"
)

func changeItemFactory1(tableName string, column1 string, column2 int64, column3 int32, column4 bool) abstract.ChangeItem {
	table1Schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column1", string(schema.TypeString), true),
		abstract.MakeTypedColSchema("column2", string(schema.TypeInt64), false),
		abstract.MakeTypedColSchema("column3", string(schema.TypeInt32), false),
		abstract.MakeTypedColSchema("column4", string(schema.TypeBoolean), false),
	})
	item1 := abstract.ChangeItem{
		Kind:         "Insert",
		Schema:       "db",
		Table:        tableName,
		ColumnNames:  []string{"column1", "column2", "column3", "column4"},
		ColumnValues: []interface{}{column1, column2, column3, column4},
		TableSchema:  table1Schema,
	}
	return item1
}

func changeItemFactory2(tableName string, column1 string, column2 time.Time, column3 float64, column4 float32) abstract.ChangeItem {
	table2Schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column1", string(schema.TypeBytes), false),
		abstract.MakeTypedColSchema("column2", string(schema.TypeDate), false),
		abstract.MakeTypedColSchema("column3", string(schema.TypeFloat64), false),
		abstract.MakeTypedColSchema("column4", string(schema.TypeFloat32), false),
	})
	item2 := abstract.ChangeItem{
		Kind:         "Insert",
		Schema:       "db",
		Table:        tableName,
		ColumnNames:  []string{"colunm1", "column2", "column3", "column4"},
		ColumnValues: []interface{}{column1, column2, column3, column4},
		TableSchema:  table2Schema,
	}
	return item2
}

func testTableSplitterOnChangeItem(
	t *testing.T,
	transformer TableSplitterTransformer,
	changeItem abstract.ChangeItem,
	expectedTableName string) {
	originalTable := changeItem.Table

	result := transformer.Apply([]abstract.ChangeItem{changeItem})
	require.Empty(t, result.Errors, "unexpected errors on transformer application")
	require.Len(t, result.Transformed, 1, "expected single result on transforming single change item")

	transformedChangeItem := result.Transformed[0]
	require.Equal(t, expectedTableName, transformedChangeItem.Table, "transformed table names are different")

	transformedChangeItemRestorationToOriginal := transformedChangeItem
	transformedChangeItemRestorationToOriginal.Table = originalTable
	require.Equal(t, transformedChangeItemRestorationToOriginal, changeItem, "transformed change item should differ only in table name")
}

func TestTableSplitterTransformer_TestChangeItemTableReplacement(t *testing.T) {
	tableSplitterFactory := func(columns []string, splitter string) TableSplitterTransformer {
		tableF, _ := filter.NewFilter(nil, []string{})
		return TableSplitterTransformer{
			Tables:      tableF,
			Columns:     columns,
			Splitter:    splitter,
			UseLegacyLF: false,
			Logger:      logger.Log,
		}
	}

	testTableSplitterOnChangeItem(t,
		tableSplitterFactory([]string{}, "_"),
		changeItemFactory1("table1", "hello", 123, 321, false),
		"table1",
	)
	testTableSplitterOnChangeItem(t,
		tableSplitterFactory([]string{"column1"}, "_"),
		changeItemFactory1("table2", "hello", 456, 654, false),
		"table2_hello",
	)
	testTableSplitterOnChangeItem(t,
		tableSplitterFactory([]string{"column1"}, "/"),
		changeItemFactory1("table3", "hello", 789, 987, false),
		"table3/hello",
	)
	testTableSplitterOnChangeItem(t,
		tableSplitterFactory([]string{"column1", "column2"}, "$"),
		changeItemFactory1("table4", "hello", 345, 543, false),
		"table4$hello$345",
	)
	testTableSplitterOnChangeItem(t,
		tableSplitterFactory([]string{"column2", "column1"}, "+"),
		changeItemFactory1("table5", "hello", 678, 876, false),
		"table5+678+hello",
	)
	testTableSplitterOnChangeItem(t,
		tableSplitterFactory([]string{"column1", "column2"}, ""),
		changeItemFactory1("table6", "helloworld", 234, 432, false),
		"table6/helloworld/234",
	)
	timeAsString := "2023-08-31T16:59:07+03:00"
	timestamp, err := time.Parse(time.RFC3339, timeAsString)
	require.NoError(t, err)
	testTableSplitterOnChangeItem(t,
		tableSplitterFactory([]string{"column4", "column2"}, "__"),
		changeItemFactory2("table7", "helloworld", timestamp, 3.14, 2.71828),
		"table7__2.71828__2023-08-31",
	)
}

func TestTableSplitterTransformer_TableIncludeExcludeList(t *testing.T) {
	tableF, _ := filter.NewFilter(nil, []string{})
	allTableSplitterTransformer := TableSplitterTransformer{
		Tables: tableF,
		Logger: logger.Log,
	}

	tableF, _ = filter.NewFilter(nil, []string{`db\.table3`})
	excludeColTransformer := TableSplitterTransformer{
		Tables: tableF,
		Logger: logger.Log,
	}

	tableF, _ = filter.NewFilter([]string{`db\.table3`}, nil)
	includeTwoColsTransformer := TableSplitterTransformer{
		Tables: tableF,
		Logger: logger.Log,
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
		Name:      "table3",
	}

	dummyTableSchema := abstract.NewTableSchema(abstract.TableColumns{})

	require.True(t, allTableSplitterTransformer.Suitable(table1, dummyTableSchema))
	require.True(t, allTableSplitterTransformer.Suitable(table2, dummyTableSchema))
	require.True(t, allTableSplitterTransformer.Suitable(table3, dummyTableSchema))

	require.True(t, excludeColTransformer.Suitable(table1, dummyTableSchema))
	require.True(t, excludeColTransformer.Suitable(table2, dummyTableSchema))
	// TODO(@kry127) TM-6564
	//require.False(t, excludeColTransformer.Suitable(table3, dummyTableSchema))

	require.False(t, includeTwoColsTransformer.Suitable(table1, dummyTableSchema))
	require.False(t, includeTwoColsTransformer.Suitable(table2, dummyTableSchema))
	require.True(t, includeTwoColsTransformer.Suitable(table3, dummyTableSchema))
}
