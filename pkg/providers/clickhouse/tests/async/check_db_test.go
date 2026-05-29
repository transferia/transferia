package async

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/middlewares"
	clickhouse_async "github.com/transferia/transferia/pkg/providers/clickhouse/async"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	"github.com/transferia/transferia/pkg/sink_factory"
	"github.com/transferia/transferia/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	source = model.MockSource{}
	target = *chrecipe.MustTarget(chrecipe.WithDatabase("test"), chrecipe.WithInitFile("init.sql"))

	targetTable = "async_test_table"
)

func init() {
	source.WithDefaults()
	target.WithDefaults()
	_ = os.Setenv("CH_LOCAL_PATH", os.Getenv("RECIPE_CLICKHOUSE_BIN"))
}

func TestAsyncSinkBatchFlush(t *testing.T) {
	// 101 MiB padding exceeds streamer's batchSizeLimit (100 MiB),
	// triggering restart() → batch.Send() which commits data to the tmp table immediately.
	const paddingSize = 101 * 1024 * 1024

	sch := abstract.NewTableSchema([]abstract.ColSchema{
		{TableName: targetTable, ColumnName: "number", DataType: ytschema.TypeInt32.String()},
		{TableName: targetTable, ColumnName: "padding", DataType: ytschema.TypeBytes.String()},
	})
	transfer := helpers.MakeTransfer(helpers.TransferID, &source, &target, abstract.TransferTypeSnapshotOnly)
	transfer.Labels = `{"dt-async-ch": "on"}`
	sink, err := sink_factory.MakeAsyncSink(transfer, &model.TransferOperation{}, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), coordinator.NewFakeClient(), middlewares.MakeConfig())
	require.NoError(t, err)
	host := &conn_clickhouse.Host{
		Name:       "localhost",
		HTTPPort:   target.HTTPPort,
		NativePort: target.NativePort,
	}
	params, err := target.ToSinkParams(transfer)
	require.NoError(t, err)
	conn, err := conn.ConnectNative(host, params)
	require.NoError(t, err)
	defer conn.Close()

	// Push InitTableLoad to init sink and create parts and target table.
	initLoadItem := []abstract.ChangeItem{{
		Kind:        abstract.InitTableLoad,
		Table:       targetTable,
		TableSchema: sch,
		PartID:      "1_1",
	}}
	require.NoError(t, <-sink.AsyncPush(initLoadItem))

	// Push data with 101 MiB padding to trigger immediate batch send.
	dataItem := []abstract.ChangeItem{{
		Kind:         abstract.InsertKind,
		Table:        targetTable,
		ColumnNames:  []string{"number", "padding"},
		ColumnValues: []any{100, make([]byte, paddingSize)},
		TableSchema:  sch,
		PartID:       "1_1",
	}}
	dataErrCh := sink.AsyncPush(dataItem) // Error or nil will be pushed to chan after DoneTableLoad.

	tmpTableName := fmt.Sprintf("%s_%s_%s_%s", clickhouse_async.TMPPrefix, "dtt", targetTable, "1_1")
	for {
		// Wait until tmp table will be filled with data.
		time.Sleep(time.Millisecond * 200)
		tables := getTablesAndRowsCount(t, conn)
		require.Zero(t, tables[targetTable]) // All data should be in tmp table, not in async_test_table.
		if len(tables) == 2 && tables[tmpTableName] > 0 {
			break
		}
	}

	// Push DoneTableLoad item on which sink will move data to target table and drop tmp table.
	doneLoadItem := []abstract.ChangeItem{{
		Kind:        abstract.DoneTableLoad,
		Table:       targetTable,
		TableSchema: sch,
		PartID:      "1_1",
	}}
	require.NoError(t, <-sink.AsyncPush(doneLoadItem))
	require.NoError(t, <-dataErrCh)

	for {
		// Wait until data will be moved to target table and tmp table dropped.
		time.Sleep(time.Millisecond * 200)
		tables := getTablesAndRowsCount(t, conn)
		if len(tables) == 1 && tables[targetTable] > 0 {
			break
		}
	}
}

// getTablesAndRowsCount returns map[tableName]rowsCount.
func getTablesAndRowsCount(t *testing.T, conn *sql.DB) map[string]uint64 {
	res := make(map[string]uint64)
	rows, err := conn.Query("SELECT name, total_rows FROM system.tables WHERE database='test'")
	require.NoError(t, err)

	name := ""
	var totalRows *uint64
	for rows.Next() {
		require.NoError(t, rows.Scan(&name, &totalRows))
		require.NotNil(t, totalRows)
		res[name] = *totalRows
	}
	return res
}
