package ch_exactly_once_snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/delivery"
	_ "github.com/transferia/transferia/tests/helpers/registration/clickhouse"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

// NewTarget gives each subtest its own database with the selected engine.
func NewTarget(test *testing.T, prefix string) *clickhouse_model.ChDestination {
	test.Helper()
	target := &clickhouse_model.ChDestination{
		User: "default", Database: "default", ChClusterName: "test_shard_localhost",
		HTTPPort:   testenv.GetIntFromEnv(prefix + "RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort: testenv.GetIntFromEnv(prefix + "RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ShardsList: []clickhouse_model.ClickHouseShard{{Name: "_", Hosts: []string{"localhost"}}},
	}
	target.WithDefaults()
	target.ProtocolUnspecified = false
	target.ExactlyOnceSnapshot = true
	target.Cleanup = model.Drop
	databaseName := "eo_" + strings.NewReplacer("/", "_", "-", "_", "=", "_", " ", "_").Replace(test.Name())
	databaseName = strings.ToLower(databaseName)
	// MongoDB sources reuse this namespace and allow at most 63 bytes.
	if len(databaseName) > 20 {
		databaseName = databaseName[:20]
	}
	databaseName += "_" + uuid.NewString()
	connection := OpenTarget(test, target)
	databaseEngine := "Atomic"
	if strings.Contains(test.Name(), "/Replicated/") {
		databaseEngine = "Replicated('/eo/" + databaseName + "', 'shard1', 'replica1')"
	}
	_, err := connection.Exec("CREATE DATABASE `" + databaseName + "` ENGINE = " + databaseEngine)
	require.NoError(test, err)
	target.Database = databaseName
	test.Cleanup(func() {
		_, err := connection.Exec("DROP DATABASE `" + databaseName + "` SYNC")
		require.NoError(test, err)
	})
	return target
}

func OpenTarget(test *testing.T, target *clickhouse_model.ChDestination) *sql.DB {
	test.Helper()
	connection, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s@localhost:%d/%s", target.User, target.NativePort, target.Database))
	require.NoError(test, err)
	test.Cleanup(func() { require.NoError(test, connection.Close()) })
	return connection
}

// Run uses the actual snapshot loader and sink factory, never direct sink.Push.
func Run(test *testing.T, source model.Source, target *clickhouse_model.ChDestination, workers int) {
	test.Helper()
	test.Setenv("YC", "1")
	require.True(test, target.ExactlyOnceSnapshot)
	transfer := transferhelpers.MakeTransfer("dtteo", source, target, abstract.TransferTypeSnapshotOnly)
	task := &model.TransferOperation{OperationID: "dtjeo", TransferID: transfer.ID, TaskType: abstract.TaskType{Task: abstract.Activate{}}, Status: model.RunningTask}
	controlPlane := delivery.NewFakeCPErrRepl()
	if workers == 1 {
		require.NoError(test, tasks.ActivateDelivery(context.Background(), task, controlPlane, *transfer, testmetrics.EmptyRegistry()))
	} else {
		transfer = transferhelpers.WithLocalRuntime(transfer, workers, 2)
		worker, err := delivery.ActivateShardedWithCP(context.Background(), controlPlane, task, transfer, testmetrics.EmptyRegistry())
		require.NoError(test, err)
		worker.Close(test)
	}
	stateKeys, err := controlPlane.GetTransferStateKeys(transfer.ID)
	require.NoError(test, err)
	for _, stateKey := range stateKeys {
		require.False(test, strings.HasPrefix(stateKey, "clickhouse_snapshot_v1/"), "snapshot left publication state after success: %s", stateKey)
	}
	// Success must include publication and cleanup, not just buffered inserts.
	connection := OpenTarget(test, target)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var temporaryTables uint64
	err = connection.QueryRowContext(ctx, "SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__dt_')", target.Database).Scan(&temporaryTables)
	require.NoError(test, err)
	require.Zero(test, temporaryTables, "snapshot left temporary tables after success")
}

// CheckRows checks physical rows without FINAL, so replacing cannot hide duplicates.
func CheckRows(test *testing.T, target *clickhouse_model.ChDestination, tableName string, expectedRows uint64) {
	test.Helper()
	connection := OpenTarget(test, target)
	var actualRows uint64
	err := connection.QueryRow("SELECT count() FROM `" + target.Database + "`.`" + tableName + "`").Scan(&actualRows)
	require.NoError(test, err)
	require.Equal(test, expectedRows, actualRows)
}

// CheckKeyPayload validates every source key and value, including duplicate keys.
func CheckKeyPayload(test *testing.T, target *clickhouse_model.ChDestination, tableName string, expectedRows int) {
	test.Helper()
	connection := OpenTarget(test, target)
	rows, err := connection.Query("SELECT id,payload FROM `" + target.Database + "`.`" + tableName + "` ORDER BY toInt64(id)")
	require.NoError(test, err)
	defer rows.Close()
	rowIndex := 0
	for rows.Next() {
		var id int64
		var payload string
		require.NoError(test, rows.Scan(&id, &payload))
		rowIndex++
		require.EqualValues(test, rowIndex, id)
		require.Equal(test, fmt.Sprintf("row-%d", rowIndex), payload)
	}
	require.NoError(test, rows.Err())
	require.Equal(test, expectedRows, rowIndex)
}

// ForDatabases runs each source scenario against both supported database engines.
func ForDatabases(test *testing.T, scenario func(*testing.T)) {
	test.Helper()
	for _, engine := range []string{"Atomic", "Replicated"} {
		test.Run(engine, scenario)
	}
}
