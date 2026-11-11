package part_uploader

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/blang/semver/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"github.com/transferia/transferia/pkg/stats"
)

func snapshotNativeTestColumns(testingHelper testing.TB, names []string, types []string) []column.Interface {
	testingHelper.Helper()
	result := make([]column.Interface, len(names))
	for columnIndex, columnName := range names {
		var err error
		result[columnIndex], err = column.Type(types[columnIndex]).Column(columnName, &column.ServerContext{Timezone: time.UTC})
		require.NoError(testingHelper, err)
	}
	return result
}

func TestSnapshotNativeRowEncoderMatchesColumnRestore(test *testing.T) {
	columns := abstract.NewTableSchema(
		[]abstract.ColSchema{
			{ColumnName: "id", DataType: "int64", Required: true},
			{ColumnName: "number", DataType: "uint64", Required: true},
			{ColumnName: "text", DataType: "utf8"},
			{ColumnName: "payload", DataType: "any"},
			{ColumnName: "timestamp", DataType: "timestamp"},
			{ColumnName: "nullable", DataType: "int64"},
			{ColumnName: "unused", DataType: "utf8"},
		},
	)
	columnNames := []string{"number", "id", "text", "payload", "timestamp", "nullable"}
	columnTypes := []string{"UInt32", "Int32", "Nullable(String)", "Nullable(String)", "Nullable(DateTime64(6))", "Nullable(Int64)"}
	item := abstract.ChangeItem{
		Kind: abstract.InsertKind, TableSchema: columns, ColumnNames: columnNames,
		ColumnValues: []any{uint64(1234), int64(-7654), "text", map[string]any{"ok": true}, time.Unix(1750000000, 123000).UTC(), nil}, CommitTime: 12345,
	}
	for _, includeTransferColumns := range []bool{false, true} {
		test.Run(fmt.Sprintf("updateable=%t", includeTransferColumns), func(test *testing.T) {
			targetNames, targetColumnTypes := slices.Clone(columnNames), slices.Clone(columnTypes)
			if includeTransferColumns {
				targetNames = append(targetNames, "__data_transfer_commit_time", "__data_transfer_delete_time")
				targetColumnTypes = append(targetColumnTypes, "UInt64", "UInt64")
			}
			encoder, err := newRowEncoder(item, snapshotNativeTestColumns(test, targetNames, targetColumnTypes), includeTransferColumns)
			require.NoError(test, err)
			expected := make([]any, len(targetNames))
			for columnIndex, columnName := range columnNames {
				for _, sourceColumn := range columns.Columns() {
					if sourceColumn.ColumnName != columnName {
						continue
					}
					targetType := columntypes.NewTypeDescription(columnTypes[columnIndex])
					if targetType.IsInteger {
						sourceColumn.DataType = targetType.YtBaseType
					}
					expected[columnIndex] = columntypes.Restore(sourceColumn, item.ColumnValues[columnIndex])
					break
				}
			}
			if includeTransferColumns {
				expected[len(columnNames)] = item.CommitTime
				expected[len(columnNames)+1] = uint64(0)
			}

			arguments, err := encoder.prepareRowArguments(&item)
			require.NoError(test, err)
			require.Equal(test, expected, arguments)
			reordered := item
			reordered.ColumnNames = slices.Clone(item.ColumnNames)
			reordered.ColumnValues = slices.Clone(item.ColumnValues)
			slices.Reverse(reordered.ColumnNames)
			slices.Reverse(reordered.ColumnValues)
			arguments, err = encoder.prepareRowArguments(&reordered)
			require.NoError(test, err)
			require.Equal(test, expected, arguments)
			require.Equal(test, "uint32", encoder.conversionColumns[0].DataType)
			require.Equal(test, "uint64", columns.Columns()[1].DataType, "source schema must not be modified")
		})
	}
}

func TestSnapshotNativeRowEncoderRejectsMalformedRows(test *testing.T) {
	schema := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "int64"}, {ColumnName: "value", DataType: "utf8"}})
	item := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: schema, ColumnNames: []string{"id", "value"}, ColumnValues: []any{int64(5), "value"}}
	encoder, err := newRowEncoder(item, snapshotNativeTestColumns(test, item.ColumnNames, []string{"Int64", "String"}), false)
	require.NoError(test, err)
	for _, columnNames := range [][]string{{"id", "id"}, {"unknown", "id"}, {"id"}} {
		invalid := item
		invalid.ColumnNames = columnNames
		_, err := encoder.prepareRowArguments(&invalid)
		require.Error(test, err)
		require.True(test, abstract.IsFatal(err))
	}
	item.Kind = abstract.DeleteKind
	_, err = encoder.prepareRowArguments(&item)
	require.ErrorContains(test, err, "instead of INSERT")
}

func TestSnapshotNativeWriterSendsOnceAndClearsScratch(test *testing.T) {
	item := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "int64"}}), ColumnNames: []string{"id"}, ColumnValues: []any{int64(15)}}
	for _, failure := range []string{"none", "prepare", "append", "send"} {
		test.Run(failure, func(test *testing.T) {
			batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"Int64"}))
			injected := xerrors.New("injected error")
			if failure == "append" {
				batch.appendErr = injected
			}
			if failure == "send" {
				batch.sendErr = injected
			}
			prepareCount := 0
			connection := newSnapshotNativeTestConnection(
				func(ctx context.Context, query string) (driver.Batch, error) {
					prepareCount++
					require.Equal(test, "INSERT INTO `db`.`table` (`id`)", query)
					_, deadlineSet := ctx.Deadline()
					require.False(test, deadlineSet, "streaming query must not expire between Push calls")
					if failure == "prepare" {
						return nil, injected
					}
					return batch, nil
				},
			)
			writer := newShardWriter(connection, "db", false, clickhouse_model.InsertParams{}.ToQueryOption(semver.MustParse("26.3.0")), stats.NewChStats(solomon.NewRegistry(nil)))
			err := writer.Write("table", []abstract.ChangeItem{item})
			if failure == "none" || failure == "send" {
				require.NoError(test, err)
				require.Zero(test, batch.sendCount, "Push must keep a small batch open")
				err = writer.Finish()
			}
			if failure == "none" {
				require.NoError(test, err)
				require.Equal(test, [][]any{{int64(15)}}, batch.rows)
			} else {
				require.ErrorIs(test, err, injected)
			}
			require.Equal(test, 1, prepareCount, "uncertain INSERT must never be retried in the same attempt")
			if failure == "none" || failure == "send" {
				require.Equal(test, 1, batch.sendCount)
			} else {
				require.Zero(test, batch.sendCount)
			}
			if failure != "prepare" {
				require.Zero(test, batch.closeCount, "Close on a native batch may commit a flushed prefix")
				require.Equal(test, []any{nil}, writer.encoder.argumentBuffer)
			}
		})
	}
}

func TestSnapshotNativeWritersRunDifferentHostsConcurrently(test *testing.T) {
	item := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "int64"}}), ColumnNames: []string{"id"}, ColumnValues: []any{int64(15)}}
	prepared := make(chan struct{}, 2)
	release := make(chan struct{})
	completed := make(chan error, 2)
	for hostIndex := 0; hostIndex < 2; hostIndex++ {
		batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"Int64"}))
		connection := newSnapshotNativeTestConnection(
			func(ctx context.Context, query string) (driver.Batch, error) {
				prepared <- struct{}{}
				<-release
				return batch, nil
			},
		)
		writer := newShardWriter(connection, "db", false, clickhouse_model.InsertParams{}.ToQueryOption(semver.MustParse("26.3.0")), stats.NewChStats(solomon.NewRegistry(nil)))
		go func() { completed <- writer.Write("table", []abstract.ChangeItem{item}) }()
	}
	defer func() {
		close(release)
		for hostIndex := 0; hostIndex < 2; hostIndex++ {
			require.NoError(test, <-completed)
		}
	}()

	for hostIndex := 0; hostIndex < 2; hostIndex++ {
		select {
		case <-prepared:
		case <-time.After(5 * time.Second):
			test.Fatal("different hosts serialized their INSERTs")
		}
	}
}

func TestSnapshotNativeWriterSequentialBatchesReleasePayload(test *testing.T) {
	item := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "value", DataType: "utf8"}}), ColumnNames: []string{"value"}, ColumnValues: []any{"first"}}
	columns := snapshotNativeTestColumns(test, item.ColumnNames, []string{"String"})
	var batches []*snapshotNativeTestBatch
	connection := newSnapshotNativeTestConnection(
		func(ctx context.Context, query string) (driver.Batch, error) {
			batch := newSnapshotNativeTestBatch(columns)
			batches = append(batches, batch)
			return batch, nil
		},
	)
	writer := newShardWriter(connection, "db", false, clickhouse_model.InsertParams{}.ToQueryOption(semver.MustParse("26.3.0")), stats.NewChStats(solomon.NewRegistry(nil)))
	require.NoError(test, writer.Write("first_table", []abstract.ChangeItem{item}))
	require.Equal(test, []any{nil}, writer.encoder.argumentBuffer)
	item.ColumnValues[0] = "second"
	require.NoError(test, writer.Write("second_table", []abstract.ChangeItem{item}))
	require.Equal(test, []any{nil}, writer.encoder.argumentBuffer)
	require.Equal(test, [][]any{{"first"}}, batches[0].rows)
	require.Equal(test, [][]any{{"second"}}, batches[1].rows)
}

func TestSnapshotNativeWriterDifferentTablesAdaptTargetIntegerWidths(test *testing.T) {
	item := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "uint64"}}), ColumnNames: []string{"id"}, ColumnValues: []any{uint64(1001)}}
	firstBatch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"UInt32"}))
	secondBatch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"UInt64"}))
	connection := newSnapshotNativeTestConnection(
		func(ctx context.Context, query string) (driver.Batch, error) {
			if query == "INSERT INTO `db`.`first_table` (`id`)" {
				return firstBatch, nil
			}
			return secondBatch, nil
		},
	)
	writer := newShardWriter(connection, "db", false, clickhouse_model.InsertParams{}.ToQueryOption(semver.MustParse("26.3.0")), stats.NewChStats(solomon.NewRegistry(nil)))
	require.NoError(test, writer.Write("first_table", []abstract.ChangeItem{item}))
	require.NoError(test, writer.Write("second_table", []abstract.ChangeItem{item}))
	require.Equal(test, [][]any{{uint32(1001)}}, firstBatch.rows)
	require.Equal(test, [][]any{{uint64(1001)}}, secondBatch.rows)
}

func TestSnapshotNativeWriterReplicaCopiesCountRowsOnce(test *testing.T) {
	item := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "int64"}}), ColumnNames: []string{"id"}, ColumnValues: []any{int64(15)}}
	logicalMetrics := stats.NewChStats(solomon.NewRegistry(nil))
	for replicaIndex := 0; replicaIndex < 2; replicaIndex++ {
		batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"Int64"}))
		connection := newSnapshotNativeTestConnection(
			func(ctx context.Context, query string) (driver.Batch, error) {
				return batch, nil
			},
		)
		var writerMetrics *stats.ChStats
		if replicaIndex == 0 {
			writerMetrics = logicalMetrics
		}
		writer := newShardWriter(connection, "db", false, clickhouse_model.InsertParams{}.ToQueryOption(semver.MustParse("26.3.0")), writerMetrics)
		require.NoError(test, writer.Write("table", []abstract.ChangeItem{item, item}))
		require.NoError(test, writer.Finish())
		require.Len(test, batch.rows, 2, "each replica must receive all rows")
		require.Equal(test, 1, batch.sendCount)
	}
	require.Equal(test, int64(2), logicalMetrics.Len.(*solomon.Counter).Value())
	require.Equal(test, int64(1), logicalMetrics.Count.(*solomon.Counter).Value())
}

func TestSnapshotNativeWriterConversionPanicIsFatal(test *testing.T) {
	item := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "timestamp", DataType: "timestamp"}}), ColumnNames: []string{"timestamp"}, ColumnValues: []any{"invalid timestamp"}}
	batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"DateTime64(6)"}))
	encoder, err := newRowEncoder(item, batch.columns, false)
	require.NoError(test, err)
	err = appendRows(batch, encoder, []abstract.ChangeItem{item})
	require.ErrorContains(test, err, "panic converting snapshot row")
	require.True(test, abstract.IsFatal(err))
	require.Zero(test, batch.sendCount)
}

func TestSnapshotNativeWriterIntegration(test *testing.T) {
	nativePort := os.Getenv("RECIPE_CLICKHOUSE_NATIVE_PORT")
	if nativePort == "" {
		test.Skip("requires the ClickHouse recipe")
	}
	connection, err := clickhouse_go.Open(&clickhouse_go.Options{Addr: []string{"127.0.0.1:" + nativePort}, Auth: clickhouse_go.Auth{Database: "default", Username: "default"}})
	require.NoError(test, err)
	test.Cleanup(func() { require.NoError(test, connection.Close()) })
	tableName := "snapshot_native_" + uuid.NewString()
	qualifiedName := ch_eo_common.QuoteIdentifier("default") + "." + ch_eo_common.QuoteIdentifier(tableName)
	require.NoError(test, connection.Exec(context.Background(), "CREATE TABLE "+qualifiedName+" (id UInt32, value Nullable(String), extra String DEFAULT 'default', __data_transfer_commit_time UInt64, __data_transfer_delete_time UInt64) ENGINE=MergeTree ORDER BY id"))
	test.Cleanup(func() { require.NoError(test, connection.Exec(context.Background(), "DROP TABLE "+qualifiedName)) })
	schema := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "uint64", Required: true}, {ColumnName: "value", DataType: "utf8"}, {ColumnName: "extra", DataType: "utf8"}})
	items := []abstract.ChangeItem{
		{Kind: abstract.InsertKind, TableSchema: schema, ColumnNames: []string{"value", "id"}, ColumnValues: []any{[]byte("one"), uint64(1001)}, CommitTime: 10},
		{Kind: abstract.InsertKind, TableSchema: schema, ColumnNames: []string{"id", "value"}, ColumnValues: []any{uint64(1002), nil}, CommitTime: 20},
	}
	// Force one native Flush before the final Send; the real server must retain
	// both the flushed block and the last buffered row exactly once.
	items[0].Size.Values = flushBytesThreshold
	writer := newShardWriter(connection, "default", true, clickhouse_model.InsertParams{}.ToQueryOption(semver.MustParse("21.3.7")), stats.NewChStats(solomon.NewRegistry(nil)))
	require.NoError(test, writer.Write(tableName, items))
	require.NoError(test, writer.Finish())
	var rowCount, commitSum, deleteSum uint64
	require.NoError(test, connection.QueryRow(context.Background(), "SELECT count(), sum(__data_transfer_commit_time), sum(__data_transfer_delete_time) FROM "+qualifiedName).Scan(&rowCount, &commitSum, &deleteSum))
	require.Equal(test, uint64(2), rowCount)
	require.Equal(test, uint64(30), commitSum)
	require.Zero(test, deleteSum)
	var firstValue, defaultValue string
	require.NoError(test, connection.QueryRow(context.Background(), "SELECT value, extra FROM "+qualifiedName+" WHERE id=1001").Scan(&firstValue, &defaultValue))
	require.Equal(test, "one", firstValue)
	require.Equal(test, "default", defaultValue)
	require.NoError(test, connection.QueryRow(context.Background(), "SELECT count() FROM "+qualifiedName+" WHERE id=1002 AND isNull(value)").Scan(&rowCount))
	require.Equal(test, uint64(1), rowCount)
}

var snapshotBenchmarkArguments []any

// Measures row conversion only. The legacy path additionally allocates
// database/sql NamedValues and two driver value slices, which are excluded here.
func BenchmarkSnapshotNativeRowArguments(benchmark *testing.B) {
	columnNames := make([]string, 32)
	columnTypes := make([]string, 32)
	columns := make([]abstract.ColSchema, 32)
	values := make([]any, 32)
	for columnIndex := range columnNames {
		columnNames[columnIndex] = fmt.Sprintf("column_%d", columnIndex)
		columnTypes[columnIndex] = "UInt64"
		columns[columnIndex] = abstract.ColSchema{ColumnName: columnNames[columnIndex], DataType: "uint64", Required: true}
		values[columnIndex] = uint64(1024 + columnIndex)
	}
	item := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: abstract.NewTableSchema(columns), ColumnNames: columnNames, ColumnValues: values, CommitTime: 12345}
	for _, valuesNeedConversion := range []bool{false, true} {
		benchmark.Run(
			fmt.Sprintf("conversion=%t", valuesNeedConversion),
			func(benchmark *testing.B) {
				if valuesNeedConversion {
					item.ColumnValues[31] = json.Number("12345")
				}
				encoder, err := newRowEncoder(item, snapshotNativeTestColumns(benchmark, columnNames, columnTypes), false)
				require.NoError(benchmark, err)
				benchmark.Run(
					"native",
					func(benchmark *testing.B) {
						benchmark.ReportAllocs()
						for iteration := 0; iteration < benchmark.N; iteration++ {
							var err error
							snapshotBenchmarkArguments, err = encoder.prepareRowArguments(&item)
							if err != nil {
								benchmark.Fatal(err)
							}
						}
					},
				)
			},
		)
	}
}

type snapshotNativeTestBatch struct {
	driver.Batch
	columns    []column.Interface
	rows       [][]any
	appendErr  error
	flushErr   error
	sendErr    error
	sendFunc   func() error
	flushCount int
	sendCount  int
	closeCount int
	abortCount int
	sent       bool
}

var _ driver.Batch = (*snapshotNativeTestBatch)(nil)

func newSnapshotNativeTestBatch(columns []column.Interface) *snapshotNativeTestBatch {
	return &snapshotNativeTestBatch{
		Batch: nil, columns: columns, rows: nil, appendErr: nil,
		flushErr: nil, sendErr: nil, sendFunc: nil, flushCount: 0, sendCount: 0, closeCount: 0, abortCount: 0, sent: false,
	}
}

func (b *snapshotNativeTestBatch) Columns() []column.Interface {
	return slices.Clone(b.columns)
}
func (b *snapshotNativeTestBatch) Append(arguments ...any) error {
	if b.appendErr != nil {
		return b.appendErr
	}
	for columnIndex, argument := range arguments {
		if err := b.columns[columnIndex].AppendRow(argument); err != nil {
			return err
		}
	}
	b.rows = append(b.rows, slices.Clone(arguments))
	return nil
}
func (b *snapshotNativeTestBatch) Send() error {
	b.sendCount++
	b.sent = true
	if b.sendFunc != nil {
		return b.sendFunc()
	}
	return b.sendErr
}
func (b *snapshotNativeTestBatch) Flush() error {
	b.flushCount++
	if b.flushErr != nil {
		return b.flushErr
	}
	for _, targetColumn := range b.columns {
		targetColumn.Reset()
	}
	return nil
}
func (b *snapshotNativeTestBatch) Abort() error {
	b.abortCount++
	b.sent = true
	return nil
}
func (b *snapshotNativeTestBatch) IsSent() bool {
	return b.sent
}
func (b *snapshotNativeTestBatch) Close() error {
	b.closeCount++
	return nil
}

type snapshotNativeTestConnection struct {
	driver.Conn
	prepare    func(context.Context, string) (driver.Batch, error)
	closeCount int
	closeErr   error
}

var _ driver.Conn = (*snapshotNativeTestConnection)(nil)

func newSnapshotNativeTestConnection(prepare func(context.Context, string) (driver.Batch, error)) *snapshotNativeTestConnection {
	return &snapshotNativeTestConnection{Conn: nil, prepare: prepare, closeCount: 0, closeErr: nil}
}

func (c *snapshotNativeTestConnection) PrepareBatch(ctx context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	return c.prepare(ctx, query)
}

func (c *snapshotNativeTestConnection) Close() error {
	c.closeCount++
	return c.closeErr
}

func snapshotNativeStreamingTestItem() abstract.ChangeItem {
	return abstract.ChangeItem{
		Kind: abstract.InsertKind, ColumnNames: []string{"value"}, ColumnValues: []any{[]byte("payload")},
		TableSchema: abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "value", DataType: "string", OriginalType: "yt:string"}}),
	}
}

func TestSnapshotNativeStreamingDefersAcknowledgement(test *testing.T) {
	item := snapshotNativeStreamingTestItem()
	var batches []*snapshotNativeTestBatch
	var batchContexts []context.Context
	connection := newSnapshotNativeTestConnection(
		func(ctx context.Context, query string) (driver.Batch, error) {
			batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"String"}))
			batches = append(batches, batch)
			batchContexts = append(batchContexts, ctx)
			return batch, nil
		},
	)
	metrics := stats.NewChStats(solomon.NewRegistry(nil))
	writer := newShardWriter(connection, "db", false, clickhouse_go.WithSettings(nil), metrics)
	require.NoError(test, writer.Write("table", []abstract.ChangeItem{item}))
	require.NoError(test, writer.Write("table", []abstract.ChangeItem{item}))
	require.Len(test, batches, 1)
	require.Equal(test, 2, batches[0].columns[0].Rows())
	require.Zero(test, batches[0].sendCount)
	require.Zero(test, metrics.Len.(*solomon.Counter).Value())
	require.NoError(test, batchContexts[0].Err(), "batch lifetime spans Push calls")
	require.Equal(test, []any{nil}, writer.encoder.argumentBuffer)
	require.NoError(test, writer.Finish())
	require.Equal(test, 1, batches[0].sendCount)
	require.Equal(test, int64(2), metrics.Len.(*solomon.Counter).Value())
	require.Equal(test, int64(1), metrics.Count.(*solomon.Counter).Value())
	require.NoError(test, batchContexts[0].Err(), "do not cancel a stopped driver watchdog after returning its connection")
	require.NoError(test, writer.Finish(), "finishing twice must not repeat INSERT")
	require.Equal(test, 1, batches[0].sendCount)
	require.NoError(test, writer.Write("table", []abstract.ChangeItem{item}))
	require.Len(test, batches, 2)
	require.NoError(test, writer.Finish())
	require.Equal(test, int64(3), metrics.Len.(*solomon.Counter).Value())
}

func TestSnapshotNativeStreamingFlushesAndAcknowledgesAllRows(test *testing.T) {
	item := snapshotNativeStreamingTestItem()
	item.Size.Values = flushBytesThreshold / 2
	batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"String"}))
	connection := newSnapshotNativeTestConnection(func(context.Context, string) (driver.Batch, error) { return batch, nil })
	metrics := stats.NewChStats(solomon.NewRegistry(nil))
	writer := newShardWriter(connection, "db", false, clickhouse_go.WithSettings(nil), metrics)
	require.NoError(test, writer.Write("table", []abstract.ChangeItem{item, item, item}))
	require.Equal(test, 1, batch.flushCount)
	require.Equal(test, 1, batch.columns[0].Rows(), "Flush resets columns before later rows are appended")
	require.Zero(test, batch.sendCount)
	require.Zero(test, metrics.Len.(*solomon.Counter).Value(), "Flush does not acknowledge an INSERT")
	require.Equal(test, uint64(3), writer.insertRows)
	require.Equal(test, uint64(1), writer.bufferedRows)
	require.NoError(test, writer.Finish())
	require.Equal(test, int64(3), metrics.Len.(*solomon.Counter).Value(), "include rows from every flushed block")
	require.Zero(test, writer.insertRows)
	require.Zero(test, writer.bufferedRows)
	require.Nil(test, writer.batch)
}

func TestSnapshotNativeStreamingLimitsRowsWithoutReportedSize(test *testing.T) {
	item := snapshotNativeStreamingTestItem()
	item.ColumnValues[0] = make([]byte, 0)
	batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"String"}))
	connection := newSnapshotNativeTestConnection(func(context.Context, string) (driver.Batch, error) { return batch, nil })
	writer := newShardWriter(connection, "db", false, clickhouse_go.WithSettings(nil), nil)
	items := make([]abstract.ChangeItem, flushRowsThreshold+3)
	for itemIndex := range items {
		items[itemIndex] = item
	}
	require.NoError(test, writer.Write("table", items))
	require.Equal(test, 1, batch.flushCount)
	require.Equal(test, 3, batch.columns[0].Rows())
	require.Equal(test, uint64(3), writer.bufferedRows)
	require.Zero(test, batch.sendCount)
	require.NoError(test, writer.Finish())
}

func TestSnapshotNativeStreamingRestartsAtQueryLimit(test *testing.T) {
	item := snapshotNativeStreamingTestItem()
	item.Size.Values = flushBytesThreshold
	var batches []*snapshotNativeTestBatch
	connection := newSnapshotNativeTestConnection(
		func(context.Context, string) (driver.Batch, error) {
			batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"String"}))
			batches = append(batches, batch)
			return batch, nil
		},
	)
	metrics := stats.NewChStats(solomon.NewRegistry(nil))
	writer := newShardWriter(connection, "db", false, clickhouse_go.WithSettings(nil), metrics)
	items := make([]abstract.ChangeItem, 11)
	for itemIndex := range items {
		items[itemIndex] = item
	}
	require.NoError(test, writer.Write("table", items))
	require.Len(test, batches, 2)
	require.Equal(test, 9, batches[0].flushCount)
	require.Equal(test, 1, batches[0].sendCount)
	require.Equal(test, 1, batches[1].flushCount)
	require.Zero(test, batches[1].sendCount)
	require.Equal(test, int64(10), metrics.Len.(*solomon.Counter).Value())
	require.Equal(test, uint64(1), writer.insertRows)
	require.NoError(test, writer.Finish())
	require.Equal(test, int64(11), metrics.Len.(*solomon.Counter).Value())
	require.Equal(test, int64(2), metrics.Count.(*solomon.Counter).Value())
}

func TestSnapshotNativeStreamingErrorsRemainSticky(test *testing.T) {
	for _, failure := range []string{"flush", "send", "append_after_flush"} {
		test.Run(failure, func(test *testing.T) {
			item := snapshotNativeStreamingTestItem()
			item.Size.Values = flushBytesThreshold
			batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"String"}))
			prepareCount := 0
			connection := newSnapshotNativeTestConnection(
				func(context.Context, string) (driver.Batch, error) {
					prepareCount++
					return batch, nil
				},
			)
			metrics := stats.NewChStats(solomon.NewRegistry(nil))
			writer := newShardWriter(connection, "db", false, clickhouse_go.WithSettings(nil), metrics)
			require.NoError(test, writer.Write("table", []abstract.ChangeItem{item}))
			injected := xerrors.New("late server error")
			var writeError error
			switch failure {
			case "flush":
				batch.flushErr = injected
				writeError = writer.Write("table", []abstract.ChangeItem{item})
			case "send":
				batch.sendErr = injected
				writeError = writer.Finish()
			case "append_after_flush":
				batch.appendErr = injected
				writeError = writer.Write("table", []abstract.ChangeItem{item})
			}
			require.ErrorIs(test, writeError, injected)
			require.ErrorIs(test, writer.Write("another_table", []abstract.ChangeItem{item}), injected)
			require.ErrorIs(test, writer.Finish(), injected)
			require.Equal(test, 1, prepareCount, "failed INSERT must not be retried")
			require.Zero(test, metrics.Len.(*solomon.Counter).Value())
			require.Zero(test, batch.closeCount, "Close could acknowledge a partial INSERT")
			if failure == "send" {
				require.Equal(test, 1, batch.sendCount)
			} else {
				require.Equal(test, 1, batch.abortCount)
			}
			require.Nil(test, writer.batch)
			require.Equal(test, []any{nil}, writer.encoder.argumentBuffer)
		})
	}
}

func TestSnapshotNativeStreamingCloseAbortsPendingRows(test *testing.T) {
	for _, flushBeforeClose := range []bool{false, true} {
		test.Run(map[bool]string{false: "buffered", true: "flushed"}[flushBeforeClose], func(test *testing.T) {
			item := snapshotNativeStreamingTestItem()
			if flushBeforeClose {
				item.Size.Values = flushBytesThreshold
			}
			batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"String"}))
			connection := newSnapshotNativeTestConnection(func(context.Context, string) (driver.Batch, error) { return batch, nil })
			writer := newShardWriter(connection, "db", false, clickhouse_go.WithSettings(nil), nil)
			require.NoError(test, writer.Write("table", []abstract.ChangeItem{item}))
			require.NoError(test, writer.Close())
			require.NoError(test, writer.Close())
			require.Equal(test, 1, batch.abortCount)
			require.Equal(test, 1, connection.closeCount)
			require.Zero(test, batch.sendCount)
			require.Zero(test, batch.closeCount)
			require.ErrorContains(test, writer.Finish(), "closed")
			require.ErrorContains(test, writer.Write("table", []abstract.ChangeItem{item}), "closed")
			require.Nil(test, writer.encoder)
		})
	}
}

func TestSnapshotNativeStreamingTableSwitchWaitsForPreviousAcknowledgement(test *testing.T) {
	item := snapshotNativeStreamingTestItem()
	batch := newSnapshotNativeTestBatch(snapshotNativeTestColumns(test, item.ColumnNames, []string{"String"}))
	injected := xerrors.New("previous table acknowledgement failed")
	batch.sendErr = injected
	prepareCount := 0
	connection := newSnapshotNativeTestConnection(
		func(context.Context, string) (driver.Batch, error) {
			prepareCount++
			return batch, nil
		},
	)
	writer := newShardWriter(connection, "db", false, clickhouse_go.WithSettings(nil), nil)
	require.NoError(test, writer.Write("first", []abstract.ChangeItem{item}))
	require.ErrorIs(test, writer.Write("second", []abstract.ChangeItem{item}), injected)
	require.Equal(test, 1, batch.sendCount)
	require.Equal(test, 1, prepareCount, "do not start another table after uncertain acknowledgement")
}
