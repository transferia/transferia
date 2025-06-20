//go:build !disable_elastic_provider

package elastic

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
)

func makeTestChangeItem(t *testing.T, colNames []string, colValues []interface{}, isKey []bool) abstract.ChangeItem {
	require.Equal(t, len(colValues), len(colNames))
	require.Equal(t, len(colValues), len(isKey))
	var schema []abstract.ColSchema
	for i := 0; i < len(colNames); i++ {
		schema = append(schema, abstract.ColSchema{PrimaryKey: isKey[i], ColumnName: colNames[i]})
	}
	return abstract.ChangeItem{
		ColumnNames:  colNames,
		ColumnValues: colValues,
		TableSchema:  abstract.NewTableSchema(schema),
	}
}

var longString = "long_string_H4JFa2uljR6bjOsLHunS6o0EiEAJejS6bPvjOECesY16GX3h4CfOAZsS7DfnDkVW3Z3cdNLmJ9W2ihy4o7RACQjxCkOyf1nnQzzxiZuid536T2c3eTDelTzpYszP21CuRWQYvq6BJs1mceZKk6HXBAeJxypW20mN96HU4LVpTOxDsfh9vL4AxMygEksIPWMjgfXoELOFRFtB2axFHU700ixmvRloVNuyVYPjbK08xbchvpEQQ6hfHM6xqBsn0SZEBmkezStJL4IRdXOosNyyLgwYgyvhU2GgdwzW9baFrr6NaJdUvZg01DEkWqPiiJBgqAtfV8dQf0vJaei0yWdYEzFt0ak23NVrDLK1pFfAiSDdisBiF9FHjbv6f7iRHvGnWeHYWAnnZMXItvjbboKXGabc0AIPrk2Hz1ydDeiAbfWTIXb3FcS0wdgIeWgfGJGFTn9tRiNcpCxoXBBVDLxdprBS7wMDKzFn2WDZnxFcjNubSrdgJjgRG9ln0JMaMhfcy"

func TestMakeIdFromChangeItem(t *testing.T) {
	testChangeItems := []abstract.ChangeItem{
		makeTestChangeItem(t, []string{"col1", "col2", "col3", "col4"}, []interface{}{"test", 0, "2", "11"}, []bool{false, false, false, false}),
		makeTestChangeItem(t, []string{"col5", "col1", "col3", "col4"}, []interface{}{"..", 0, "2", ".."}, []bool{true, false, false, false}),
		makeTestChangeItem(t, []string{"col1", "col2", "col4"}, []interface{}{longString, "", 13.122}, []bool{true, false, true}),
		makeTestChangeItem(t, []string{"col8", "col2", "col3", "col4"}, []interface{}{"{\"name\":123}", -.221, "some(&^)value", "string.with.dots"}, []bool{false, true, true, false}),
		makeTestChangeItem(t, []string{"col2", "col1", "col7", "col4"}, []interface{}{"test", 0, "", "11"}, []bool{false, true, true, false}),
		makeTestChangeItem(t, []string{"col6"}, []interface{}{".te\\/st."}, []bool{true}),
		makeTestChangeItem(t, []string{"col6"}, []interface{}{"test."}, []bool{true}),
		makeTestChangeItem(t, []string{"col6", "col2"}, []interface{}{"test", "."}, []bool{true, true}),
		makeTestChangeItem(t, []string{"col6", "col2"}, []interface{}{"test", ""}, []bool{true, true}),
		makeTestChangeItem(t, []string{"col6", "col2"}, []interface{}{"test\\", nil}, []bool{true, true}),
	}
	var canonArr []string
	for _, testChangeItem := range testChangeItems {
		canonArr = append(canonArr, makeIDFromChangeItem(testChangeItem))
	}
	canon.SaveJSON(t, canonArr)
}

func TestSanitizeKeysInRawJSON(t *testing.T) {
	t.Parallel()
	testJSONs := []string{
		`{"_rest": {
        "find_writer_stat": {
            ".{\"cluster\":\"vla\",\"partition\":180,\"topic\":\"strm-stream/strm-access-log\"}": "4.043µs"
        },
        "write_stat": {
            ".{\"cluster\":\"vla\",\"partition\":180,\"topic\":\"strm-stream/strm-access-log\"}": "277.590725ms"
        }}}`,
		`{
           "_rest": {
            "#all_messages": 1,
            "#bytes": 6816,
            "#change_items": 1,
            "dst_id": "-watcher-abc_watcher_prod",
            "dst_type": "lb",
            "duration": "129.291µs",
            "job_id": "1f084078-cedecdd1-3f60384-8ab",
            "logical_job_index": "0",
            "revision": "10946848",
            "src_id": "src_id-3501-4751-9d10-ad600dc20cf1",
            "src_type": "pg",
            "stat_by_messages": {
                ".": 1
            },
            "stat_by_size": {
                ".": 6816
            },
            "yt_operation_id": "yt_operation_id-234-234-242-4"
        }
    }`,
		`{". . . a .b":"test_1"}`,
		`{"a..b.cc":"test_2"}`,
		`{"a... . .b. .":"test_3"}`,
		`{"a ":"test_4"}`,
		`{".a":"test_5"}`,
		`{" a":"test_6"}`,
		`{"a     b":"test_7"}`,
		`{"....key....":"test_8"}`,
		`{"s o m e.. k e y... ":"test_9"}`,
		`{"":"test_10"}`,
		`{"   ":"test_11"}`,
		`{".":"test_12"}`,
	}
	var canonArr []string
	for _, testJSON := range testJSONs {
		out, err := sanitizeKeysInRawJSON([]byte(testJSON))
		require.NoError(t, err)
		canonArr = append(canonArr, string(out))
	}
	canon.SaveJSON(t, canonArr)
}
