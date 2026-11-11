package part_uploader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
)

func snapshotNativeEncodedColumns(testingHelper testing.TB, columns []column.Interface) []byte {
	testingHelper.Helper()
	buffer := &proto.Buffer{}
	for _, targetColumn := range columns {
		if serialization, ok := targetColumn.(column.CustomSerialization); ok {
			require.NoError(testingHelper, serialization.WriteStatePrefix(buffer))
		}
		targetColumn.Encode(buffer)
	}
	return buffer.Buf
}

// Compare the native wire representation: []byte and string are deliberately
// different Go values, but must encode to the same ClickHouse column values.
func TestSnapshotNativeStringFastPathMatchesLegacyEncoding(test *testing.T) {
	binaryValue := []byte{0, 255, 'a', 128, 0, 'z'}
	stringValue := "pointer string"
	for _, testCase := range []struct {
		name         string
		dataType     string
		originalType string
		values       []any
	}{
		{name: "yt_binary", dataType: "string", originalType: "yt:string", values: []any{binaryValue, make([]byte, 0), []byte(nil), "ordinary", "", nil, &stringValue}},
		{name: "yt_utf8", dataType: "utf8", originalType: "yt:utf8", values: []any{[]byte("utf8 text"), "plain text", nil}},
		{name: "generic_string", dataType: "string", values: []any{binaryValue, "generic", uint64(42), map[string]any{"key": "value"}}},
		{name: "mysql_binary", dataType: "string", originalType: "mysql:binary(8)", values: []any{"AP9hgAB6", binaryValue, nil}},
		{name: "mysql_varbinary", dataType: "string", originalType: "mysql:varbinary(8)", values: []any{"AP9hgAB6", binaryValue, nil}},
		{name: "mysql_blob", dataType: "string", originalType: "mysql:blob", values: []any{"AP9hgAB6", binaryValue, nil}},
		{name: "mysql_bit", dataType: "string", originalType: "mysql:bit(8)", values: []any{json.Number("17"), "bit text", []byte("17")}},
		{name: "pg_bytea", dataType: "string", originalType: "pg:bytea", values: []any{"AP9hgAB6", binaryValue, nil}},
		{name: "ydb_string", dataType: "string", originalType: "ydb:String", values: []any{"AP9hgAB6", binaryValue, nil}},
		{name: "json_any", dataType: "any", values: []any{map[string]any{"key": "value"}, []byte("json bytes"), "json text", nil}},
	} {
		for _, targetType := range []string{"String", "Nullable(String)"} {
			test.Run(testCase.name+"/"+targetType, func(test *testing.T) {
				sourceColumn := abstract.ColSchema{ColumnName: "value", DataType: testCase.dataType, OriginalType: testCase.originalType}
				item := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: abstract.NewTableSchema([]abstract.ColSchema{sourceColumn}), ColumnNames: []string{"value"}}
				nativeColumns := snapshotNativeTestColumns(test, item.ColumnNames, []string{targetType})
				legacyColumns := snapshotNativeTestColumns(test, item.ColumnNames, []string{targetType})
				encoder, err := newRowEncoder(item, nativeColumns, false)
				require.NoError(test, err)
				for _, value := range testCase.values {
					item.ColumnValues = []any{value}
					arguments, err := encoder.prepareRowArguments(&item)
					require.NoError(test, err)
					legacyValue := columntypes.Restore(sourceColumn, value)
					legacyError := legacyColumns[0].AppendRow(legacyValue)
					nativeError := nativeColumns[0].AppendRow(arguments[0])
					require.NoError(test, legacyError)
					require.NoError(test, nativeError)
				}
				require.Equal(test, snapshotNativeEncodedColumns(test, legacyColumns), snapshotNativeEncodedColumns(test, nativeColumns))
			})
		}
	}
}

func TestSnapshotNativeStringFastPathKeepsLowCardinalityConversion(test *testing.T) {
	for _, targetType := range []string{"LowCardinality(String)", "LowCardinality(Nullable(String))"} {
		test.Run(targetType, func(test *testing.T) {
			sourceColumn := abstract.ColSchema{ColumnName: "value", DataType: "string", OriginalType: "yt:string"}
			item := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: abstract.NewTableSchema([]abstract.ColSchema{sourceColumn}), ColumnNames: []string{"value"}}
			nativeColumns := snapshotNativeTestColumns(test, item.ColumnNames, []string{targetType})
			legacyColumns := snapshotNativeTestColumns(test, item.ColumnNames, []string{targetType})
			encoder, err := newRowEncoder(item, nativeColumns, false)
			require.NoError(test, err)
			for _, value := range []any{[]byte{0, 255, 'x'}, make([]byte, 0), "repeated", "repeated", nil} {
				item.ColumnValues = []any{value}
				arguments, err := encoder.prepareRowArguments(&item)
				require.NoError(test, err)
				require.NoError(test, legacyColumns[0].AppendRow(columntypes.Restore(sourceColumn, value)))
				require.NoError(test, nativeColumns[0].AppendRow(arguments[0]))
			}
			require.Equal(test, snapshotNativeEncodedColumns(test, legacyColumns), snapshotNativeEncodedColumns(test, nativeColumns))
		})
	}
}

func TestSnapshotNativeStringFastPathCopiesIntoDriverColumns(test *testing.T) {
	for _, targetType := range []string{"String", "Nullable(String)"} {
		test.Run(targetType, func(test *testing.T) {
			payload := []byte{0, 255, 'x'}
			item := abstract.ChangeItem{
				Kind: abstract.InsertKind, ColumnNames: []string{"value"}, ColumnValues: []any{payload},
				TableSchema: abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "value", DataType: "string", OriginalType: "yt:string"}}),
			}
			nativeColumns := snapshotNativeTestColumns(test, item.ColumnNames, []string{targetType})
			legacyColumns := snapshotNativeTestColumns(test, item.ColumnNames, []string{targetType})
			encoder, err := newRowEncoder(item, nativeColumns, false)
			require.NoError(test, err)
			arguments, err := encoder.prepareRowArguments(&item)
			require.NoError(test, err)
			require.IsType(test, make([]byte, 0), arguments[0])
			require.NoError(test, legacyColumns[0].AppendRow(string(payload)))
			require.NoError(test, nativeColumns[0].AppendRow(arguments[0]))
			payload[0] = 'z'
			require.Equal(test, snapshotNativeEncodedColumns(test, legacyColumns), snapshotNativeEncodedColumns(test, nativeColumns))
		})
	}
}

func TestSnapshotNativeStringFastPathReordersColumns(test *testing.T) {
	item := abstract.ChangeItem{
		Kind: abstract.InsertKind, ColumnNames: []string{"text", "binary", "id"}, ColumnValues: []any{[]byte("text"), "YmluYXJ5", uint64(70000)},
		TableSchema: abstract.NewTableSchema(
			[]abstract.ColSchema{
				{ColumnName: "text", DataType: "utf8", OriginalType: "yt:utf8"},
				{ColumnName: "binary", DataType: "string", OriginalType: "pg:bytea"},
				{ColumnName: "id", DataType: "uint64"},
			},
		),
	}
	columnTypes := []string{"Nullable(String)", "String", "UInt32"}
	nativeColumns := snapshotNativeTestColumns(test, item.ColumnNames, columnTypes)
	legacyColumns := snapshotNativeTestColumns(test, item.ColumnNames, columnTypes)
	encoder, err := newRowEncoder(item, nativeColumns, false)
	require.NoError(test, err)
	for columnIndex, sourceColumn := range encoder.conversionColumns {
		require.NoError(test, legacyColumns[columnIndex].AppendRow(columntypes.Restore(sourceColumn, item.ColumnValues[columnIndex])))
	}
	item.ColumnNames = slices.Clone(item.ColumnNames)
	item.ColumnValues = slices.Clone(item.ColumnValues)
	slices.Reverse(item.ColumnNames)
	slices.Reverse(item.ColumnValues)
	arguments, err := encoder.prepareRowArguments(&item)
	require.NoError(test, err)
	for columnIndex, value := range arguments {
		require.NoError(test, nativeColumns[columnIndex].AppendRow(value))
	}
	require.Equal(test, snapshotNativeEncodedColumns(test, legacyColumns), snapshotNativeEncodedColumns(test, nativeColumns))
}

// Models a 1 KiB YT row represented by []byte cells. Both variants reuse the
// argument slice and driver columns; the only difference is the conversion path.
// Protocol encoding, network I/O and batch-buffer growth are excluded.
func BenchmarkSnapshotNativeStringRows(benchmark *testing.B) {
	columnNames := make([]string, 16)
	columnTypes := make([]string, len(columnNames))
	sourceColumns := make([]abstract.ColSchema, len(columnNames))
	values := make([]any, len(columnNames))
	for columnIndex := range columnNames {
		columnNames[columnIndex] = fmt.Sprintf("column_%d", columnIndex)
		columnTypes[columnIndex] = "Nullable(String)"
		sourceColumns[columnIndex] = abstract.ColSchema{ColumnName: columnNames[columnIndex], DataType: "string", OriginalType: "yt:string"}
		values[columnIndex] = bytes.Repeat([]byte{byte(columnIndex)}, 64)
	}
	item := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: abstract.NewTableSchema(sourceColumns), ColumnNames: columnNames, ColumnValues: values}
	for _, useFastPath := range []bool{false, true} {
		benchmark.Run(
			fmt.Sprintf("fast_path=%t", useFastPath),
			func(benchmark *testing.B) {
				targetColumns := snapshotNativeTestColumns(benchmark, columnNames, columnTypes)
				encoder, err := newRowEncoder(item, targetColumns, false)
				require.NoError(benchmark, err)
				if !useFastPath {
					clear(encoder.stringPassthrough)
				}
				benchmark.SetBytes(1024)
				benchmark.ReportAllocs()
				benchmark.ResetTimer()
				for iteration := 0; iteration < benchmark.N; iteration++ {
					arguments, err := encoder.prepareRowArguments(&item)
					if err != nil {
						benchmark.Fatal(err)
					}
					for columnIndex, value := range arguments {
						if err := targetColumns[columnIndex].AppendRow(value); err != nil {
							benchmark.Fatal(err)
						}
						targetColumns[columnIndex].Reset()
					}
				}
			},
		)
	}
}
