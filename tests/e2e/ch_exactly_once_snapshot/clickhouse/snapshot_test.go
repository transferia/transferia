package clickhouse

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/worker/tasks"
	eo "github.com/transferia/transferia/tests/e2e/ch_exactly_once_snapshot"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

func TestHomogeneousSnapshotIgnoresExactlyOnceFlag(test *testing.T) {
	test.Setenv("YC", "1")
	source := chrecipe.MustSource(chrecipe.WithDatabase("default"))
	sourceTarget := chrecipe.MustTarget(chrecipe.WithDatabase("default"))
	connection := eo.OpenTarget(test, sourceTarget)
	_, err := connection.Exec(`CREATE TABLE rows (
		id Int64,
		payload String,
		payload_length UInt64 MATERIALIZED length(payload),
		payload_alias String ALIAS concat(payload, '-alias')
	) ENGINE=MergeTree ORDER BY id`)
	require.NoError(test, err)
	_, err = connection.Exec("INSERT INTO rows (id, payload) SELECT toInt64(number+1), concat('row-',toString(number+1)) FROM numbers(257)")
	require.NoError(test, err)
	_, err = connection.Exec("CREATE TABLE empty (id Int64,payload String) ENGINE=MergeTree ORDER BY id")
	require.NoError(test, err)
	source.IncludeTables = []string{"default.rows", "default.empty"}
	eo.ForDatabases(test, func(test *testing.T) {
		for _, format := range []clickhouse_model.ClickhouseIOFormat{clickhouse_model.ClickhouseIOFormatCSV, clickhouse_model.ClickhouseIOFormatJSONCompact} {
			for _, workers := range []int{1, 2} {
				for _, exactlyOnceSnapshot := range []bool{false, true} {
					test.Run(fmt.Sprintf("%s/workers%d/exactlyOnceSnapshot=%t", format, workers, exactlyOnceSnapshot), func(test *testing.T) {
						currSource := *source
						currSource.IOHomoFormat = format
						target := eo.NewTarget(test, "DB0_")
						target.ChClusterName = "" // The local recipe has one server and no configured cluster.
						target.ExactlyOnceSnapshot = exactlyOnceSnapshot
						transfer := transferhelpers.MakeTransfer("dtthomo", &currSource, target, abstract.TransferTypeSnapshotOnly)
						require.True(test, transfer.IsAbstract2(), "the flag must keep the homogeneous snapshot route")
						task := new(model.TransferOperation)
						task.OperationID = "dtjhomo"
						task.TransferID = transfer.ID
						task.TaskType.Task = abstract.Activate{}
						task.Status = model.RunningTask
						controlPlane := delivery.NewFakeCPErrRepl()
						if workers == 1 {
							require.NoError(test, tasks.ActivateDelivery(context.Background(), task, controlPlane, *transfer, testmetrics.EmptyRegistry()))
						} else {
							transfer = transferhelpers.WithLocalRuntime(transfer, workers, 2)
							worker, err := delivery.ActivateShardedWithCP(context.Background(), controlPlane, task, transfer, testmetrics.EmptyRegistry())
							require.NoError(test, err)
							worker.Close(test)
						}
						eo.CheckRows(test, target, "rows", 257)
						eo.CheckRows(test, target, "empty", 0)
						eo.CheckKeyPayload(test, target, "rows", 257)

						targetConnection := eo.OpenTarget(test, target)
						for _, column := range []struct {
							name string
							kind string
						}{
							{name: "payload_length", kind: "MATERIALIZED"},
							{name: "payload_alias", kind: "ALIAS"},
						} {
							var sourceKind string
							var sourceExpression string
							err := connection.QueryRow("SELECT default_kind, default_expression FROM system.columns WHERE database = 'default' AND table = 'rows' AND name = ?", column.name).Scan(&sourceKind, &sourceExpression)
							require.NoError(test, err)
							require.Equal(test, column.kind, sourceKind)
							var kind string
							var expression string
							err = targetConnection.QueryRow("SELECT default_kind, default_expression FROM system.columns WHERE database = ? AND table = 'rows' AND name = ?", target.Database, column.name).Scan(&kind, &expression)
							require.NoError(test, err)
							require.Equal(test, sourceKind, kind)
							require.Equal(test, sourceExpression, expression)
						}
						rows, err := targetConnection.Query("SELECT payload_length, payload_alias FROM `" + target.Database + "`.rows ORDER BY id")
						require.NoError(test, err)
						defer rows.Close()
						rowIndex := 0
						for rows.Next() {
							var payloadLength uint64
							var payloadAlias string
							require.NoError(test, rows.Scan(&payloadLength, &payloadAlias))
							rowIndex++
							payload := fmt.Sprintf("row-%d", rowIndex)
							require.Equal(test, uint64(len(payload)), payloadLength)
							require.Equal(test, payload+"-alias", payloadAlias)
						}
						require.NoError(test, rows.Err())
						require.Equal(test, 257, rowIndex)
					})
				}
			}
		}
	})
}
