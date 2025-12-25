package filterrows

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	yts "go.ytsaurus.tech/yt/go/schema"
)

// makeChangeItems creates Insert into db.table items.
func makeChangeItems(schema *abstract.TableSchema, kind abstract.Kind, values [][]interface{}) []abstract.ChangeItem {
	items := make([]abstract.ChangeItem, len(values))
	for i := range items {
		items[i] = abstract.ChangeItem{
			Kind: kind, Schema: "db", Table: "table", ColumnNames: schema.ColumnNames(),
			ColumnValues: values[i], TableSchema: schema,
			OldKeys: abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
		}
	}
	return items
}

func prepareInputExpected(t *testing.T, tr *FilterRowsTransformer, table abstract.TableID, columnType string, input, expected [][]interface{}) (items, expectedItems []abstract.ChangeItem) {
	tableSchema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column", columnType, true),
	})
	checkSuitable(t, tr, table, tableSchema)

	items = makeChangeItems(tableSchema, abstract.InsertKind, input)
	expectedItems = makeChangeItems(tableSchema, abstract.InsertKind, expected)
	return
}

func checkSuitable(t *testing.T, tr *FilterRowsTransformer, table abstract.TableID, tableSchema *abstract.TableSchema) {
	require.True(t, tr.Suitable(table, tableSchema))
	res, err := tr.ResultSchema(tableSchema)
	require.NoError(t, err)
	require.Equal(t, tableSchema, res)
}

func TestIntFiltering(t *testing.T) {
	filter := "column > 10 AND column <= 15 AND column IN (11, 15)"
	tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
	require.NoError(t, err)
	table := abstract.TableID{Namespace: "db", Name: "table"}

	t.Run("int8", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeInt8.String(),
			[][]interface{}{{math.MinInt8}, {10}, {11}, {14}, {15}, {16}, {math.MaxInt8}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("int16", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeInt16.String(),
			[][]interface{}{{math.MinInt16}, {10}, {11}, {14}, {15}, {16}, {math.MaxInt16}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("int32", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeInt32.String(),
			[][]interface{}{{math.MinInt32}, {10}, {11}, {14}, {15}, {16}, {math.MaxInt32}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("int64", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeInt64.String(),
			[][]interface{}{{math.MinInt64}, {10}, {11}, {14}, {15}, {16}, {math.MaxInt64}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("uint8", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeUint8.String(),
			[][]interface{}{{0}, {10}, {11}, {14}, {15}, {16}, {math.MaxUint8}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("uint16", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeUint16.String(),
			[][]interface{}{{0}, {10}, {11}, {14}, {15}, {16}, {math.MaxUint16}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("uint32", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeUint32.String(),
			[][]interface{}{{0}, {10}, {11}, {14}, {15}, {16}, {math.MaxUint32}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("uint64", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeUint64.String(),
			[][]interface{}{{0}, {10}, {11}, {14}, {15}, {16}, {math.MaxInt64}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("uint64-int-overflow", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeUint64.String(),
			[][]interface{}{{0}, {10}, {11}, {14}, {15}, {16}, {uint64(math.MaxUint64)}},
			[][]interface{}{{11}, {15}},
		)

		result := tr.Apply(items)

		require.Equal(t, expected, result.Transformed)
		require.Equal(t, 1, len(result.Errors))
		require.ErrorIs(t, result.Errors[0].Error, errIntOverflow)
	})
}

func TestFloatFiltering(t *testing.T) {
	filter := "column >= 10.1 AND column < 15.3 AND column NOT IN (15.2, 11.0)"
	tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
	require.NoError(t, err)
	table := abstract.TableID{Namespace: "db", Name: "table"}

	t.Run("float32", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeFloat32.String(),
			[][]interface{}{{-1.0}, {10.09}, {10.1}, {11.0}, {14.0}, {15.0}, {15.2}, {15.29}, {15.3}, {16.0}, {math.MaxFloat32}},
			[][]interface{}{{10.1}, {14.0}, {15.0}, {15.29}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("float64", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeFloat64.String(),
			[][]interface{}{{-1.0}, {10.09}, {10.1}, {11.0}, {14.0}, {15.0}, {15.2}, {15.29}, {15.3}, {16.0}, {math.MaxFloat32}},
			[][]interface{}{{10.1}, {14.0}, {15.0}, {15.29}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})
}

func TestIntFilteringByFloat(t *testing.T) {
	filter := "column > 10.1 AND column <= 15.1 AND column IN (11.0, 15.0)"
	tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
	require.NoError(t, err)
	table := abstract.TableID{Namespace: "db", Name: "table"}

	t.Run("int8", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeInt8.String(),
			[][]interface{}{{math.MinInt8}, {10}, {11}, {14}, {15}, {16}, {math.MaxInt8}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("int16", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeInt16.String(),
			[][]interface{}{{math.MinInt16}, {10}, {11}, {14}, {15}, {16}, {math.MaxInt16}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("int32", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeInt32.String(),
			[][]interface{}{{math.MinInt32}, {10}, {11}, {14}, {15}, {16}, {math.MaxInt32}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("int64", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeInt64.String(),
			[][]interface{}{{math.MinInt64}, {10}, {11}, {14}, {15}, {16}, {math.MaxInt64}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("uint8", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeUint8.String(),
			[][]interface{}{{0}, {10}, {11}, {14}, {15}, {16}, {math.MaxUint8}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("uint16", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeUint16.String(),
			[][]interface{}{{0}, {10}, {11}, {14}, {15}, {16}, {math.MaxUint16}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("uint32", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeUint32.String(),
			[][]interface{}{{0}, {10}, {11}, {14}, {15}, {16}, {math.MaxUint32}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("uint64", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeUint64.String(),
			[][]interface{}{{0}, {10}, {11}, {14}, {15}, {16}, {math.MaxInt64}},
			[][]interface{}{{11}, {15}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("uint64-int-overflow", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeUint64.String(),
			[][]interface{}{{0}, {10}, {11}, {14}, {15}, {16}, {uint64(math.MaxUint64)}},
			[][]interface{}{{11}, {15}},
		)

		result := tr.Apply(items)

		require.Equal(t, expected, result.Transformed)
		require.Equal(t, 1, len(result.Errors))
		require.ErrorIs(t, result.Errors[0].Error, errIntOverflow)
	})
}

func TestFloatFilteringByInt(t *testing.T) {
	filter := "column >= 10 AND column < 15 AND column IN (10.0, 11.0, 14.9)"
	tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
	require.NoError(t, err)
	table := abstract.TableID{Namespace: "db", Name: "table"}

	t.Run("float32", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeFloat32.String(),
			[][]interface{}{{-1.0}, {10.0}, {10.1}, {11.0}, {14.0}, {14.9}, {15.0}, {math.MaxFloat32}},
			[][]interface{}{{10.0}, {11.0}, {14.9}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("float64", func(t *testing.T) {
		items, expected := prepareInputExpected(t, tr, table, yts.TypeFloat64.String(),
			[][]interface{}{{-1.0}, {10.0}, {10.1}, {11.0}, {14.0}, {14.9}, {15.0}, {math.MaxFloat64}},
			[][]interface{}{{10.0}, {11.0}, {14.9}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})
}

func TestBoolFiltering(t *testing.T) {
	table := abstract.TableID{Namespace: "db", Name: "table"}

	t.Run("bool1", func(t *testing.T) {
		filter := "column = true AND column != false AND column > false AND column >= false AND column <= true"
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeBoolean.String(),
			[][]interface{}{{true}, {false}},
			[][]interface{}{{true}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("bool2", func(t *testing.T) {
		filter := "column = false AND column != true AND column < true AND column >= false AND column <= false"
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeBoolean.String(),
			[][]interface{}{{true}, {false}},
			[][]interface{}{{false}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})
}

func TestNullFiltering(t *testing.T) {
	table := abstract.TableID{Namespace: "db", Name: "table"}

	filter := "column1 != NULL AND column2 = NULL"
	tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
	require.NoError(t, err)

	tableSchema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("id", yts.TypeInt32.String(), true),
		abstract.MakeTypedColSchema("column1", yts.TypeString.String(), false),
		abstract.MakeTypedColSchema("column2", yts.TypeInt64.String(), false),
	})
	checkSuitable(t, tr, table, tableSchema)

	items := makeChangeItems(tableSchema, abstract.InsertKind, [][]interface{}{
		{1, "abc", 128},
		{2, "str", nil},
		{3, nil, 32},
		{4, nil, nil},
	})
	expected := makeChangeItems(tableSchema, abstract.InsertKind, [][]interface{}{
		// {1, "abc", 128},
		{2, "str", nil},
		// {3, nil, 32},
		// {4, nil, nil},
	})

	expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
	require.Equal(t, expectedResult, tr.Apply(items))
}

func TestTimeFiltering(t *testing.T) {
	time1 := time.Date(1986, time.April, 26, 1, 23, 47, 0, time.FixedZone("UTC+3", 3*3600))
	time2 := time.Date(1990, time.July, 22, 0, 0, 0, 0, time.FixedZone("UTC+4", 4*3600))
	time3 := time2.Add(time.Millisecond)
	time4 := time.Date(1991, time.December, 26, 0, 0, 0, 0, time.FixedZone("UTC+3", 3*3600))
	time5 := time.Date(2003, time.April, 17, 10, 19, 0, 0, time.FixedZone("UTC+3", 3*3600))
	time6 := time5.Add(time.Millisecond)

	filter := fmt.Sprintf("column >= %s AND column < %s AND column NOT IN (%s)", time2.Format(time.RFC3339Nano), time6.Format(time.RFC3339Nano), time3.Format(time.RFC3339Nano))
	tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
	require.NoError(t, err)
	table := abstract.TableID{Namespace: "db", Name: "table"}

	items, expected := prepareInputExpected(t, tr, table, yts.TypeTimestamp.String(),
		[][]interface{}{{time1}, {time2}, {time3}, {time4}, {time5}, {time6}},
		[][]interface{}{{time2}, {time4}, {time5}},
	)

	expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
	require.Equal(t, expectedResult, tr.Apply(items))
}

func TestStringFiltering(t *testing.T) {
	table := abstract.TableID{Namespace: "db", Name: "table"}

	t.Run("=", func(t *testing.T) {
		filter := `column = "str"`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)
		items, expected := prepareInputExpected(t, tr, table, yts.TypeString.String(),
			[][]interface{}{{"str"}, {"st"}, {"strr"}, {"tr"}, {""}, {`"`}, {`""`}},
			[][]interface{}{{"str"}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("!=", func(t *testing.T) {
		filter := `column != "str"`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeString.String(),
			[][]interface{}{{"str"}, {"st"}, {"strr"}, {"tr"}, {""}, {`"`}, {`""`}},
			[][]interface{}{{"st"}, {"strr"}, {"tr"}, {""}, {`"`}, {`""`}},
		)
		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run(">", func(t *testing.T) {
		filter := `column > "abc"`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeString.String(),
			[][]interface{}{{"ab"}, {"abc"}, {"abca"}, {"abcz"}, {"abd"}, {"ac"}, {""}, {`"`}, {`""`}},
			[][]interface{}{{"abca"}, {"abcz"}, {"abd"}, {"ac"}},
		)
		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run(">=", func(t *testing.T) {
		filter := `column >= "abc"`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeString.String(),
			[][]interface{}{{"ab"}, {"abc"}, {"abca"}, {"abcz"}, {"abd"}, {"ac"}, {""}, {`"`}, {`""`}},
			[][]interface{}{{"abc"}, {"abca"}, {"abcz"}, {"abd"}, {"ac"}},
		)
		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("<", func(t *testing.T) {
		filter := `column < "bcd"`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeString.String(),
			[][]interface{}{{"ab"}, {"bcc"}, {"bccz"}, {"bcd"}, {"bcda"}, {"bce"}, {""}, {`"`}, {`""`}},
			[][]interface{}{{"ab"}, {"bcc"}, {"bccz"}, {""}, {`"`}, {`""`}},
		)
		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("<=", func(t *testing.T) {
		filter := `column <= "bcd"`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeString.String(),
			[][]interface{}{{"ab"}, {"bcc"}, {"bccz"}, {"bcd"}, {"bcda"}, {"bce"}, {""}, {`"`}, {`""`}},
			[][]interface{}{{"ab"}, {"bcc"}, {"bccz"}, {"bcd"}, {""}, {`"`}, {`""`}},
		)
		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("~ (Match / Substring)", func(t *testing.T) {
		filter := `column ~ "str"`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeString.String(),
			[][]interface{}{{"str"}, {"st"}, {"sstr"}, {"sttr"}, {"strr"}, {"astrb"}, {"rts"}, {""}, {`"`}, {`""`}},
			[][]interface{}{{"str"}, {"sstr"}, {"strr"}, {"astrb"}},
		)
		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("!~ (Not match / Not substring)", func(t *testing.T) {
		filter := `column !~ "str"`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeString.String(),
			[][]interface{}{{"str"}, {"st"}, {"sstr"}, {"sttr"}, {"strr"}, {"astrb"}, {"rts"}, {""}, {`"`}, {`""`}},
			[][]interface{}{{"st"}, {"sttr"}, {"rts"}, {""}, {`"`}, {`""`}},
		)
		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("in", func(t *testing.T) {
		filter := `column IN ('str', '"')`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeString.String(),
			[][]interface{}{{"str"}, {"st"}, {"strr"}, {"tr"}, {""}, {`"`}, {`""`}},
			[][]interface{}{{"str"}, {`"`}},
		)
		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})
}

func TestBytesFiltering(t *testing.T) {
	table := abstract.TableID{Namespace: "db", Name: "table"}

	t.Run("=", func(t *testing.T) {
		filter := `column = "str"`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)
		items, expected := prepareInputExpected(t, tr, table, yts.TypeBytes.String(),
			[][]interface{}{{[]byte("str")}, {[]byte("st")}, {[]byte("strr")}, {[]byte("tr")}, {[]byte("")}, {[]byte(`"`)}, {[]byte(`""`)}},
			[][]interface{}{{[]byte("str")}},
		)

		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("!=", func(t *testing.T) {
		val := []byte{17, 22, 15}
		filter := fmt.Sprintf("column != \"%s\"", string(val))
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeBytes.String(),
			[][]interface{}{{[]byte{12, 7, 31, 52}}, {[]byte{7, 14, 27, 43}}, {[]byte{17, 22, 15}}, {[]byte{15}}, {[]byte{43, 21, 15, 2}}},
			[][]interface{}{{[]byte{12, 7, 31, 52}}, {[]byte{7, 14, 27, 43}}, {[]byte{15}}, {[]byte{43, 21, 15, 2}}},
		)
		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run(">", func(t *testing.T) {
		filter := `column > "abc"`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeBytes.String(),
			[][]interface{}{{[]byte("ab")}, {[]byte("abc")}, {[]byte("abca")}, {[]byte("abcz")}, {[]byte("abd")}, {[]byte("ac")}, {[]byte("")}, {[]byte(`"`)}, {[]byte(`""`)}},
			[][]interface{}{{[]byte("abca")}, {[]byte("abcz")}, {[]byte("abd")}, {[]byte("ac")}},
		)
		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("~ (Match / Substring)", func(t *testing.T) {
		filter := `column ~ "☺"`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeString.String(),
			[][]interface{}{{[]byte("☺str")}, {[]byte("st")}, {[]byte("ss☺tr")}, {[]byte("sttr")}, {[]byte("strr☺")}, {[]byte("ast☺rb")}, {[]byte("rts")}, {[]byte("")}, {[]byte(`"`)}, {[]byte(`""`)}},
			[][]interface{}{{[]byte("☺str")}, {[]byte("ss☺tr")}, {[]byte("strr☺")}, {[]byte("ast☺rb")}},
		)
		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})

	t.Run("in", func(t *testing.T) {
		filter := `column IN ('str', '"', '☺')`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)

		items, expected := prepareInputExpected(t, tr, table, yts.TypeString.String(),
			[][]interface{}{{[]byte("str")}, {[]byte("st")}, {[]byte("strr")}, {[]byte("tr")}, {[]byte("")}, {[]byte(`"`)}, {[]byte(`""`)}, {[]byte(`☺`)}},
			[][]interface{}{{[]byte("str")}, {[]byte(`"`)}, {[]byte(`☺`)}},
		)
		expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
		require.Equal(t, expectedResult, tr.Apply(items))
	})
}

func TestErrors(t *testing.T) {
	table := abstract.TableID{Namespace: "db", Name: "table"}

	tableSchema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("colstr", yts.TypeString.String(), false),
		abstract.MakeTypedColSchema("colint", yts.TypeInt32.String(), false),
	})

	t.Run("Compare different types", func(t *testing.T) {
		filter := `colstr > 10 AND colint = "str"`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)
		require.False(t, tr.Suitable(table, tableSchema))
		resSchema, err := tr.ResultSchema(tableSchema)
		require.NoError(t, err)
		require.Equal(t, tableSchema, resSchema)
		items := makeChangeItems(tableSchema, abstract.InsertKind, [][]interface{}{
			{"str", 1}, {"abc", 2},
		})
		res := tr.Apply(items)
		require.Empty(t, res.Transformed)
		require.Len(t, res.Errors, 2)
	})

	t.Run("Table doesn't contain one of columns", func(t *testing.T) {
		filter := `colstr = "str" AND colint > 4`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)
		require.True(t, tr.Suitable(table, tableSchema))
		items := makeChangeItems(tableSchema, abstract.InsertKind, [][]interface{}{
			{"str", 1},
			{"abc", 10},
			{"str", 10},
		})

		badSchema1 := abstract.NewTableSchema(abstract.TableColumns{
			abstract.MakeTypedColSchema("colstr", yts.TypeString.String(), false),
		})
		require.False(t, tr.Suitable(table, badSchema1))
		items = append(items, makeChangeItems(badSchema1, abstract.InsertKind, [][]interface{}{
			{"str"},
		})...)

		badSchema2 := abstract.NewTableSchema(abstract.TableColumns{
			abstract.MakeTypedColSchema("colint", yts.TypeInt32.String(), false),
		})
		require.False(t, tr.Suitable(table, badSchema2))
		items = append(items, makeChangeItems(badSchema2, abstract.InsertKind, [][]interface{}{
			{10},
		})...)

		expectedItems := makeChangeItems(tableSchema, abstract.InsertKind, [][]interface{}{
			{"str", 10},
		})

		res := tr.Apply(items)
		require.Equal(t, expectedItems, res.Transformed)
		require.Len(t, res.Errors, 2)
	})

	t.Run("Unparseable filter", func(t *testing.T) {
		filter := `column = str"`
		_, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.Error(t, err)
	})

	t.Run("Impossible kinds (update or delete)", func(t *testing.T) {
		filter := `colstr != ""`
		tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
		require.NoError(t, err)
		require.True(t, tr.Suitable(table, tableSchema))
		resSchema, err := tr.ResultSchema(tableSchema)
		require.NoError(t, err)
		require.Equal(t, tableSchema, resSchema)
		items := makeChangeItems(tableSchema, abstract.InsertKind, [][]interface{}{
			{"str", 1}, {"abc", 2},
		})
		items = append(items, makeChangeItems(tableSchema, abstract.UpdateKind, [][]interface{}{
			{"str", 1}, {"abc", 2},
		})...)
		items = append(items, makeChangeItems(tableSchema, abstract.DeleteKind, [][]interface{}{
			{"str", 1}, {"abc", 2},
		})...)
		res := tr.Apply(items)
		require.Len(t, res.Transformed, 2)
		require.Len(t, res.Errors, 4)
	})
}

func TestTablesAndColumnsFiltering(t *testing.T) {
	includedTable := abstract.TableID{Namespace: "db", Name: "table"}
	excludedTable := abstract.TableID{Namespace: "db", Name: "table-excluded"}

	filtr := "column = 3"
	tables := filter.Tables{ExcludeTables: []string{excludedTable.Name}}

	// Excluded column stands for such column that is not mentioned in the filter.
	includedSchema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column", yts.TypeInt32.String(), true),
		abstract.MakeTypedColSchema("excluded-column", yts.TypeString.String(), false),
	})

	// Note that excluded table has column with name same as we filtering but it has different type.
	// This table excluded, so there should be no error about type difference.
	excludedSchema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column", yts.TypeString.String(), true),
		abstract.MakeTypedColSchema("excluded-column", yts.TypeInt32.String(), false),
	})

	tr, err := NewFilterRowsTransformer(Config{Tables: tables, Filter: filtr}, logger.Log)
	require.NoError(t, err)
	require.True(t, tr.Suitable(includedTable, includedSchema))
	require.False(t, tr.Suitable(excludedTable, excludedSchema))
	res, err := tr.ResultSchema(includedSchema)
	require.NoError(t, err)
	require.Equal(t, includedSchema, res)

	excludedItem := abstract.ChangeItem{
		Kind: "Insert", Schema: "db", Table: "table-excluded", ColumnNames: nil,
		ColumnValues: []interface{}{"abc", 5}, TableSchema: excludedSchema,
		OldKeys: abstract.OldKeysType{KeyNames: nil, KeyTypes: nil, KeyValues: nil},
	}
	items := append(
		makeChangeItems(includedSchema, abstract.InsertKind, [][]interface{}{
			{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}, {5, "e"},
		}),
		excludedItem,
	)
	expected := append(
		makeChangeItems(includedSchema, abstract.InsertKind, [][]interface{}{
			{3, "c"},
		}),
		excludedItem,
	)
	expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
	require.Equal(t, expectedResult, tr.Apply(items))
}

func TestCoupleInFilters(t *testing.T) {
	filter := "column IN (10, 11, 14, 15) AND column NOT IN (10) AND column IN (11, 15) AND column NOT IN (10, 11)"
	tr, err := NewFilterRowsTransformer(Config{Filter: filter}, logger.Log)
	require.NoError(t, err)
	table := abstract.TableID{Namespace: "db", Name: "table"}

	items, expected := prepareInputExpected(t, tr, table, yts.TypeInt8.String(),
		[][]interface{}{{math.MinInt8}, {10}, {11}, {14}, {15}, {16}, {math.MaxInt8}},
		[][]interface{}{{15}},
	)

	expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
	require.Equal(t, expectedResult, tr.Apply(items))
}

func TestManyFilters(t *testing.T) {
	filters := []string{
		"column1 > 10",
		`column2 = "str"`,
	}
	tr, err := NewFilterRowsTransformer(Config{Filters: filters}, logger.Log)
	require.NoError(t, err)

	schema := abstract.NewTableSchema(abstract.TableColumns{
		abstract.MakeTypedColSchema("column1", yts.TypeInt8.String(), true),
		abstract.MakeTypedColSchema("column2", yts.TypeString.String(), true),
	})
	checkSuitable(t, tr, abstract.TableID{}, schema)

	columnValues := [][]any{
		{15, "str"}, // Matches both filters.
		{15, "aaa"}, // Matches filter (column1 > 10).
		{5, "str"},  // Matches filter (column2 = "str").
		{5, "aaa"},  // Doesn't match any of the filters.
	}
	items := make([]abstract.ChangeItem, len(columnValues))
	for i := range items {
		items[i] = abstract.ChangeItem{
			Kind: abstract.InsertKind, ColumnNames: schema.ColumnNames(),
			ColumnValues: columnValues[i], TableSchema: schema,
		}
	}
	expected := []abstract.ChangeItem{items[0], items[1], items[2]}
	expectedResult := abstract.TransformerResult{Transformed: expected, Errors: []abstract.TransformerError{}}
	require.Equal(t, expectedResult, tr.Apply(items))
}
