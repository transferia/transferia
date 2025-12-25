package abstract

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTypedChangeItem(t *testing.T) {
	t.Setenv("TZ", "NZ") // see: https://github.com/golang/go/issues/45960
	ci := &TypedChangeItem{
		ID:          291975574,
		CommitTime:  1601382119000000000,
		Kind:        UpdateKind,
		Schema:      "ppc",
		Table:       "camp_options",
		TableSchema: NewTableSchema([]ColSchema{{PrimaryKey: true, ColumnName: "cid"}}),
		ColumnNames: []string{
			"cid",
			"meaningful_goals",
		},
		ColumnValues: []interface{}{
			123,
			321,
			int8(-10),
			int16(-1000),
			int32(-1000000),
			int64(-10000000000),
			uint8(10),
			uint16(1000),
			uint32(1000000),
			uint64(10000000000),
			float32(1.2),
			float64(1.2),
			json.Number("11267650600228229401496703205376"),
			json.Number("1126765.0600228229401496703205376"),
			bool(false),
			string("string"),
			[]byte("string"),
			time.Unix(0, 1658866078090000000),
			[]interface{}{uint8(10), []interface{}{"test tuple", time.Unix(0, 1658866078090000000)}, []interface{}{int64(1), interface{}("a")}},
			[]interface{}{0, "test variant (unnamed)"},
			[]interface{}{"vs_string", "test variant (named)"},
			map[string]interface{}{"key": "value"},
		},
	}
	jsonT, err := json.MarshalIndent(ci, "", "   ")
	require.NoError(t, err)
	fmt.Println(string(jsonT))
	var restoredCI TypedChangeItem
	require.NoError(t, json.Unmarshal(jsonT, &restoredCI))
	require.Equal(t, ci.ColumnValues, restoredCI.ColumnValues)
	require.Equal(t, *ci, restoredCI)
}
