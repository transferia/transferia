package todatetime

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestToDateTimeTransformer(t *testing.T) {
	t.Parallel()
	tableF, _ := filter.NewFilter(nil, []string{})
	colF, _ := filter.NewFilter([]string{}, nil)
	noColsTransformer := ToDateTimeTransformer{
		Tables:  tableF,
		Columns: colF,
		Logger:  logger.Log,
	}

	tableF, _ = filter.NewFilter(nil, nil)
	colF, _ = filter.NewFilter([]string{"column2", "column3"}, nil)
	includeTwoColsTransformer := ToDateTimeTransformer{
		Tables:  tableF,
		Columns: colF,
		Logger:  logger.Log,
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
		abstract.MakeTypedColSchema("column2", string(schema.TypeInt32), false),
		abstract.MakeTypedColSchema("column3", string(schema.TypeInt16), false),
	})

	table2Schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column1", string(schema.TypeUint32), false),
		abstract.MakeTypedColSchema("column2", string(schema.TypeDatetime), false),
		abstract.MakeTypedColSchema("column3", string(schema.TypeFloat32), false),
	})

	table3Schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column2", string(schema.TypeInt8), false),
		abstract.MakeTypedColSchema("column3", string(schema.TypeUint32), false),
		abstract.MakeTypedColSchema("column4", string(schema.TypeDatetime), false),
	})

	require.False(t, noColsTransformer.Suitable(table1, table1Schema))
	require.False(t, noColsTransformer.Suitable(table2, table2Schema))
	require.False(t, noColsTransformer.Suitable(table3, table3Schema))

	require.True(t, includeTwoColsTransformer.Suitable(table1, table1Schema))
	require.False(t, includeTwoColsTransformer.Suitable(table2, table2Schema))
	require.True(t, includeTwoColsTransformer.Suitable(table3, table3Schema))

	item1 := abstract.ChangeItem{
		Kind:         "Insert",
		Schema:       "db",
		Table:        "table1",
		ColumnNames:  []string{"column1", "column2", "column3"},
		ColumnValues: []interface{}{"value1", int32(1759143061), int16(1234)},
		TableSchema:  table1Schema,
	}

	item2 := abstract.ChangeItem{
		Kind:        "Insert",
		Schema:      "db",
		Table:       "table2",
		ColumnNames: []string{"colunm1", "column2", "column3"},
		ColumnValues: []interface{}{
			uint32(1759143071),
			time.Date(2025, 9, 29, 11, 22, 0, 0, time.UTC),
			123.123,
		},
		TableSchema: table2Schema,
	}
	item3 := abstract.ChangeItem{
		Kind:         "Insert",
		Schema:       "db",
		Table:        "a_table3",
		ColumnNames:  []string{"column2", "column3", "column4"},
		ColumnValues: []interface{}{int8(-3), uint32(1759143081), time.Date(2025, 9, 29, 10, 0, 0, 0, time.UTC)},
		TableSchema:  table3Schema,
	}
	var result []abstract.TransformerResult
	for _, transformer := range []ToDateTimeTransformer{noColsTransformer, includeTwoColsTransformer} {
		for _, item := range []abstract.ChangeItem{item1, item2, item3} {
			if transformer.Suitable(item.TableID(), item.TableSchema) {
				result = append(result, transformer.Apply([]abstract.ChangeItem{item}))
			}
		}
	}

	canon.SaveJSON(t, result)
}

func TestAllTypesToDateTimeTransformer(t *testing.T) {
	t.Parallel()
	var testCases = []struct {
		originalValue interface{}
		originalType  schema.Type
		expectedValue time.Time
	}{
		{int32(1759143081), schema.TypeInt32, time.Unix(int64(1759143081), 0)},
		{uint32(1759143061), schema.TypeUint32, time.Unix(int64(1759143061), 0)},
	}
	for _, testCase := range testCases {
		require.Equal(t, testCase.expectedValue, SerializeToDateTime(testCase.originalValue, testCase.originalType.String()))
	}
}

func TestResultSchema(t *testing.T) {
	t.Parallel()

	cols := []abstract.ColSchema{
		{ColumnName: "ColInt64", DataType: schema.TypeInt64.String()},
		{ColumnName: "ColInt32", DataType: schema.TypeInt32.String()},
		{ColumnName: "ColInt16", DataType: schema.TypeInt16.String()},
		{ColumnName: "ColUint64", DataType: schema.TypeUint64.String()},
		{ColumnName: "ColUint32", DataType: schema.TypeUint32.String()},
		{ColumnName: "ColUint16", DataType: schema.TypeUint16.String()},
		{ColumnName: "ColDatetime", DataType: schema.TypeDatetime.String()},
	}
	input := abstract.NewTableSchema(cols)
	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.ColumnName
	}

	t.Run("orig-schema-not-changed", func(t *testing.T) {
		colsFilter, err := filter.NewFilter([]string{}, nil) // Transform nothing.
		require.NoError(t, err)
		transformer := ToDateTimeTransformer{Columns: colsFilter}
		output, err := transformer.ResultSchema(input)
		require.NoError(t, err)
		require.False(t, input.Equal(output))
	})

	t.Run("one-column-included", func(t *testing.T) {
		testColIdx := 1
		expectedCols := input.Columns().Copy()
		expectedCols[testColIdx].DataType = schema.TypeDatetime.String()
		expected := abstract.NewTableSchema(expectedCols)

		colsFilter, err := filter.NewFilter([]string{colNames[testColIdx]}, nil) // Include only `testColIdx` column.
		require.NoError(t, err)
		transformer := ToDateTimeTransformer{Columns: colsFilter}
		actual, err := transformer.ResultSchema(input)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	})

	t.Run("all-supported-included", func(t *testing.T) {
		supportedColTypes := []string{schema.TypeInt32.String(), schema.TypeUint32.String()}
		expectedCols := input.Columns().Copy()
		colsForFilter := []string{}
		for i := range expectedCols {
			if slices.Contains(supportedColTypes, expectedCols[i].DataType) {
				expectedCols[i].DataType = schema.TypeDatetime.String()
				colsForFilter = append(colsForFilter, expectedCols[i].ColumnName)
			}
		}
		expected := abstract.NewTableSchema(expectedCols)

		colsFilter, err := filter.NewFilter(slices.Clone(colsForFilter), nil)
		require.NoError(t, err)
		transformer := ToDateTimeTransformer{Columns: colsFilter}
		actual, err := transformer.ResultSchema(input)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	})
}
