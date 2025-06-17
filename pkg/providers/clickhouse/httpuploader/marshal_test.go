//go:build !disable_clickhouse_provider

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
