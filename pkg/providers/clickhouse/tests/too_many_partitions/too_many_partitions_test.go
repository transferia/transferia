package toomanypartitions

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	"github.com/transferia/transferia/pkg/sink_factory"
	"github.com/transferia/transferia/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	targetTable = "too_many_partitions_test"
	rowsCount   = 150 // must exceed the server default max_partitions_per_insert_block (100)
)

var (
	source = model.MockSource{}
	target = *chrecipe.MustTarget(chrecipe.WithDatabase("test"), chrecipe.WithInitFile("init.sql"))
)

func init() {
	source.WithDefaults()
	target.WithDefaults()
	_ = os.Setenv("CH_LOCAL_PATH", os.Getenv("RECIPE_CLICKHOUSE_BIN"))
}

// The target table is partitioned by id, so a single batch of rowsCount rows spans rowsCount
// partitions and is rejected by the server with "Too many partitions for single INSERT block"
// (code 252). The sink must retry the batch with the limit lifted instead of getting stuck (TM-9213).
func TestTooManyPartitionsInsertIsRetried(t *testing.T) {
	sch := abstract.NewTableSchema([]abstract.ColSchema{
		{TableName: targetTable, ColumnName: "id", DataType: ytschema.TypeInt64.String(), PrimaryKey: true, Required: true},
		{TableName: targetTable, ColumnName: "value", DataType: ytschema.TypeString.String()},
	})
	items := make([]abstract.ChangeItem, rowsCount)
	for i := range items {
		items[i] = abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			Table:        targetTable,
			ColumnNames:  []string{"id", "value"},
			ColumnValues: []any{int64(i), fmt.Sprintf("value_%d", i)},
			TableSchema:  sch,
		}
	}

	transfer := helpers.MakeTransfer(helpers.TransferID, &source, &target, abstract.TransferTypeSnapshotOnly)
	sink, err := sink_factory.MakeAsyncSink(transfer, &model.TransferOperation{}, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), coordinator.NewFakeClient(), middlewares.MakeConfig())
	require.NoError(t, err)

	params, err := target.ToSinkParams(transfer)
	require.NoError(t, err)
	host := &conn_clickhouse.Host{Name: "localhost", HTTPPort: target.HTTPPort, NativePort: target.NativePort}
	db, err := conn.ConnectNative(host, params)
	require.NoError(t, err)
	defer db.Close()

	var limit uint64
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT toUInt64(value) FROM system.settings WHERE name = 'max_partitions_per_insert_block'`).Scan(&limit))
	require.Less(t, limit, uint64(rowsCount), "server default max_partitions_per_insert_block must be below rowsCount, otherwise the test is useless")

	require.NoError(t, <-sink.AsyncPush(items))

	var rows uint64
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT count() FROM test.too_many_partitions_test`).Scan(&rows))
	require.Equal(t, uint64(rowsCount), rows)

	var partitions uint64
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT uniqExact(partition) FROM system.parts WHERE database = 'test' AND table = 'too_many_partitions_test' AND active`).Scan(&partitions))
	require.Equal(t, uint64(rowsCount), partitions)
}
