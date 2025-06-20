//go:build !disable_clickhouse_provider

package snapshot

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	chconn "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/middlewares"
	ch_async "github.com/transferia/transferia/pkg/providers/clickhouse/async"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	"github.com/transferia/transferia/pkg/sink"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/schema"
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

func TestTransformerTypeInference(t *testing.T) {
	sch := abstract.NewTableSchema([]abstract.ColSchema{{
		TableName:  targetTable,
		ColumnName: "number",
		DataType:   schema.TypeInt32.String(),
	}})
	transfer := helpers.MakeTransfer(helpers.TransferID, &source, &target, abstract.TransferTypeSnapshotOnly)
	transfer.Labels = `{"dt-async-ch": "on"}`
	sink, err := sink.MakeAsyncSink(transfer, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), coordinator.NewFakeClient(), middlewares.MakeConfig())
	require.NoError(t, err)
	host := &chconn.Host{
		Name:       "localhost",
		HTTPPort:   target.HTTPPort,
		NativePort: target.NativePort,
	}
	params, err := target.ToSinkParams(transfer)
	require.NoError(t, err)
	conn, err := conn.ConnectNative(host, params)
	require.NoError(t, err)
	defer conn.Close()

	// Push InitShardedTableLoad to init sink and create parts and target table.
	initLoadItem := []abstract.ChangeItem{{
		Kind:        abstract.InitTableLoad,
		Table:       targetTable,
		TableSchema: sch,
		PartID:      "1_1",
	}}
	require.NoError(t, <-sink.AsyncPush(initLoadItem))

	// Push data.
	dataItem := []abstract.ChangeItem{{
		Kind:         abstract.InsertKind,
		Table:        targetTable,
		ColumnNames:  []string{"number"},
		ColumnValues: []any{100},
		TableSchema:  sch,
		PartID:       "1_1",
		Size: abstract.EventSize{
			// Set fake data size to reach size of batch that will be immediately pushed to tmp table.
			Read: math.MaxInt32,
		},
	}}
	dataErrCh := sink.AsyncPush(dataItem) // Error or nil will be pushed to chan after DoneShardedTableLoad.

	tmpTableName := fmt.Sprintf("%s_%s_%s_%s", ch_async.TMPPrefix, "dtt", targetTable, "1_1")
	for {
		// Wait until tmp table will be filled with data.
		time.Sleep(time.Millisecond * 200)
		tables := getTablesAndRowsCount(t, conn)
		require.Zero(t, tables[targetTable]) // All data should be in tmp table, not in async_test_table.
		if len(tables) == 2 && tables[tmpTableName] > 0 {
			break
		}
	}

	// Push DoneShardedTableLoad item on which sink will move data to target table and drop tmp table.
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
