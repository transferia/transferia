package httpuploader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestDatetime64Marshal(t *testing.T) {
	testSpec := func(chType, etalon string) func(t *testing.T) {
		return func(t *testing.T) {
			buf := bytes.Buffer{}
			marshalTime(
				columntypes.NewTypeDescription(chType),
				time.Date(2020, 2, 2, 10, 2, 22, 123456789, time.UTC),
				&buf,
			)
			require.Equal(t, etalon, buf.String())
		}
	}
	t.Run("DateTime64(1)", testSpec("DateTime64(1)", "15806377421"))
	t.Run("DateTime64(2)", testSpec("DateTime64(2)", "158063774212"))
	t.Run("DateTime64(3)", testSpec("DateTime64(3)", "1580637742123"))
	t.Run("DateTime64(4)", testSpec("DateTime64(4)", "15806377421234"))
	t.Run("DateTime64(5)", testSpec("DateTime64(5)", "158063774212345"))
	t.Run("DateTime64(6)", testSpec("DateTime64(6)", "1580637742123456"))
	t.Run("DateTime64(7)", testSpec("DateTime64(7)", "15806377421234567"))
	t.Run("DateTime64(8)", testSpec("DateTime64(8)", "158063774212345678"))
	t.Run("DateTime64(9)", testSpec("DateTime64(9)", "1580637742123456789"))
}

func TestValidJSON(t *testing.T) {
	tschema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "bytes_with_jsons", DataType: ytschema.TypeString.String()},
	})

	for i, tc := range []struct {
		value string
	}{
		{value: `"[{\"foo\":0}]"`},
		{value: `[{\"foo\":0}]`},
		{value: `"[{\\\"foo\":0}]`},
	} {
		t.Run(fmt.Sprintf("tc_%v", i), func(t *testing.T) {
			buf := &bytes.Buffer{}
			require.NoError(t, MarshalCItoJSON(abstract.ChangeItem{
				ColumnNames:  []string{"bytes_with_jsons"},
				ColumnValues: []any{[]byte(tc.value)},
				TableSchema:  tschema,
			}, NewRules(
				tschema.ColumnNames(),
				tschema.Columns(),
				abstract.MakeMapColNameToIndex(tschema.Columns()),
				map[string]*columntypes.TypeDescription{
					"bytes_with_jsons": new(columntypes.TypeDescription),
				},
				false,
			), buf))
			fmt.Printf("\n%v", buf.String())
			var r map[string]string

			require.NoError(t, json.Unmarshal(buf.Bytes(), &r))
			require.Equal(t, tc.value, r["bytes_with_jsons"])
		})
	}
}

func TestEscapNotCurraptNonUTF8(t *testing.T) {
	tschema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "bytes_with_jsons", DataType: ytschema.TypeString.String()},
	})

	value := append(append([]byte(`"Hello`), byte(254)), []byte("世")...)
	buf := &bytes.Buffer{}
	require.NoError(t, MarshalCItoJSON(abstract.ChangeItem{
		ColumnNames:  []string{"bytes_with_jsons"},
		ColumnValues: []any{value},
		TableSchema:  tschema,
	}, NewRules(
		tschema.ColumnNames(),
		tschema.Columns(),
		abstract.MakeMapColNameToIndex(tschema.Columns()),
		map[string]*columntypes.TypeDescription{
			"bytes_with_jsons": new(columntypes.TypeDescription),
		},
		false,
	), buf))
	fmt.Printf("\n%v", buf.String())
	hackyByteInPlace := false
	for _, b := range buf.Bytes() {
		if b == byte(254) {
			hackyByteInPlace = true
		}
	}
	require.True(t, hackyByteInPlace)
}

func TestNullValueMarshal(t *testing.T) {
	tschema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "recipient", DataType: ytschema.TypeString.String()},
	})

	t.Run("single_null_column", func(t *testing.T) {
		buf := &bytes.Buffer{}
		err := MarshalCItoJSON(abstract.ChangeItem{
			ColumnNames:  []string{"recipient"},
			ColumnValues: []any{nil},
			TableSchema:  tschema,
		}, NewRules(
			tschema.ColumnNames(),
			tschema.Columns(),
			abstract.MakeMapColNameToIndex(tschema.Columns()),
			map[string]*columntypes.TypeDescription{
				"recipient": new(columntypes.TypeDescription),
			},
			false,
		), buf)

		require.NoError(t, err)
		result := buf.String()

		// Should be valid JSON object, even if empty
		require.Equal(t, "{}\n", result, "Expected empty JSON object with newline")

		// Verify it's valid JSON
		var parsed map[string]interface{}
		err = json.Unmarshal(buf.Bytes(), &parsed)
		require.NoError(t, err, "Result should be valid JSON")
		require.Empty(t, parsed, "Parsed object should be empty")
	})

	t.Run("multiple_columns_all_null", func(t *testing.T) {
		tschema := abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "col1", DataType: ytschema.TypeString.String()},
			{ColumnName: "col2", DataType: ytschema.TypeString.String()},
		})

		buf := &bytes.Buffer{}
		err := MarshalCItoJSON(abstract.ChangeItem{
			ColumnNames:  []string{"col1", "col2"},
			ColumnValues: []any{nil, nil},
			TableSchema:  tschema,
		}, NewRules(
			tschema.ColumnNames(),
			tschema.Columns(),
			abstract.MakeMapColNameToIndex(tschema.Columns()),
			map[string]*columntypes.TypeDescription{
				"col1": new(columntypes.TypeDescription),
				"col2": new(columntypes.TypeDescription),
			},
			false,
		), buf)

		require.NoError(t, err)
		result := buf.String()

		// Should be valid JSON object, even if empty
		require.Equal(t, "{}\n", result, "Expected empty JSON object with newline")

		// Verify it's valid JSON
		var parsed map[string]interface{}
		err = json.Unmarshal(buf.Bytes(), &parsed)
		require.NoError(t, err, "Result should be valid JSON")
		require.Empty(t, parsed, "Parsed object should be empty")
	})

	t.Run("mixed_null_and_values", func(t *testing.T) {
		tschema := abstract.NewTableSchema([]abstract.ColSchema{
			{ColumnName: "col1", DataType: ytschema.TypeString.String()},
			{ColumnName: "col2", DataType: ytschema.TypeString.String()},
			{ColumnName: "col3", DataType: ytschema.TypeString.String()},
		})

		buf := &bytes.Buffer{}
		err := MarshalCItoJSON(abstract.ChangeItem{
			ColumnNames:  []string{"col1", "col2", "col3"},
			ColumnValues: []any{nil, "value2", nil},
			TableSchema:  tschema,
		}, NewRules(
			tschema.ColumnNames(),
			tschema.Columns(),
			abstract.MakeMapColNameToIndex(tschema.Columns()),
			map[string]*columntypes.TypeDescription{
				"col1": new(columntypes.TypeDescription),
				"col2": new(columntypes.TypeDescription),
				"col3": new(columntypes.TypeDescription),
			},
			false,
		), buf)

		require.NoError(t, err)
		result := buf.String()

		// Should contain only col2
		require.Equal(t, `{"col2":"value2"}`+"\n", result, "Expected JSON with only non-null column")

		// Verify it's valid JSON
		var parsed map[string]interface{}
		err = json.Unmarshal(buf.Bytes(), &parsed)
		require.NoError(t, err, "Result should be valid JSON")
		require.Equal(t, "value2", parsed["col2"])
	})
}
