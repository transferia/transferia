package clickhouse

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	connection "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	chmodel "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/transfer_state"
)

type snapshotAdapterCoordinator struct {
	*coordinator.CoordinatorInMemory
}

func TestSnapshotAdapterRejectsAsyncOperationsBeforeOpeningDestination(t *testing.T) {
	provider := &Provider{transfer: &model.Transfer{AsyncOperations: true, Dst: &chmodel.ChDestination{ExactlyOnceSnapshot: true}}}
	_, err := provider.SnapshotSink(middlewares.Config{})
	require.ErrorContains(t, err, "does not support AsyncOperations")
	require.True(t, abstract.IsFatal(err))
}

func TestSnapshotAdapterEmptyCleanupNeedsNoDestination(t *testing.T) {
	provider := &Provider{cp: snapshotAdapterCoordinator{coordinator.NewStatefulFakeClient()}, transfer: &model.Transfer{ID: "empty-cleanup", Dst: &chmodel.ChDestination{ExactlyOnceSnapshot: true}}}
	require.NoError(t, provider.CleanupDestination(context.Background()))
}

func TestHomogeneousSnapshotCleanupDoesNotReadPublicationJournal(t *testing.T) {
	cp := coordinator.NewFakeClientWithOpts(func(string) (map[string]*coordinator.TransferStateData, error) {
		t.Fatal("homogeneous snapshot cleanup must not read the exactly-once publication journal")
		return nil, nil
	}, nil, nil)
	transfer := new(model.Transfer)
	transfer.Src = new(chmodel.ChSource)
	transfer.Dst = &chmodel.ChDestination{ExactlyOnceSnapshot: true}
	provider := &Provider{cp: cp, transfer: transfer}
	require.NoError(t, provider.CleanupDestination(context.Background()))
}

func TestSnapshotAdapterRejectsIncrementalCursorBeforeOpeningDestination(t *testing.T) {
	provider := &Provider{transfer: &model.Transfer{RegularSnapshot: &abstract.RegularSnapshot{Incremental: []abstract.IncrementalTable{{}}}, Dst: &chmodel.ChDestination{ExactlyOnceSnapshot: true}}}
	_, err := provider.SnapshotSink(middlewares.Config{})
	require.ErrorContains(t, err, "does not support incremental snapshot cursors")
	require.True(t, abstract.IsFatal(err))
}

func newSnapshotAdapterNativeProvider(t *testing.T, alternativeNames []model.AltName) (*Provider, *sql.DB) {
	t.Helper()
	if os.Getenv("DT_SNAPSHOT_CLICKHOUSE_TEST") != "1" {
		t.Skip("requires ClickHouse on 127.0.0.1:19000/18123")
	}
	dst := &chmodel.ChDestination{Database: "default", Hosts: []string{"127.0.0.1"}, User: "default", NativePort: 19000, HTTPPort: 18123, ExactlyOnceSnapshot: true, IsUpdateable: true, Cleanup: model.Drop}
	dst.AltNamesList = alternativeNames
	dst.ShardsList = []chmodel.ClickHouseShard{{Name: "s1", Hosts: []string{"127.0.0.1"}}}
	dst.WithDefaults()
	transfer := &model.Transfer{ID: "snapshot-adapter", Dst: dst, Runtime: &abstract.LocalRuntime{}, Type: abstract.TransferTypeSnapshotAndIncrement}
	params, err := dst.ToSinkParams(transfer)
	require.NoError(t, err)
	db, err := conn.ConnectNative(&connection.Host{Name: "127.0.0.1", NativePort: 19000, HTTPPort: 18123}, params)
	require.NoError(t, err)
	database := "dt_snapshot_adapter_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = db.Exec("CREATE DATABASE " + database + " ENGINE = Atomic")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := db.Exec("DROP DATABASE " + database + " SYNC")
		require.NoError(t, err)
		require.NoError(t, db.Close())
	})
	dst.Database = database
	cp := snapshotAdapterCoordinator{coordinator.NewStatefulFakeClient()}
	op := &model.TransferOperation{OperationID: "snapshot-operation"}
	provider := &Provider{logger: logger.Log, registry: emptyRegistry(), cp: cp, transfer: transfer, operation: op}
	t.Cleanup(provider.CloseSnapshotOperation)
	return provider, db
}

func TestSnapshotAdapterUploadCleanupWithoutSchema(t *testing.T) {
	for _, policy := range []model.CleanupType{model.Drop, model.Truncate} {
		t.Run(string(policy), func(t *testing.T) {
			provider, db := newSnapshotAdapterNativeProvider(t, nil)
			destination := provider.transfer.Dst.(*chmodel.ChDestination)
			destination.Cleanup = policy
			destination.IsUpdateable = false
			provider.operation.OperationID = "upload-operation"
			provider.operation.TaskType = abstract.TaskType{Task: abstract.Upload{}}
			ctx := context.Background()
			_, err := db.Exec("CREATE TABLE " + destination.Database + ".t (id Int64, value String) ENGINE=MergeTree ORDER BY id")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO " + destination.Database + ".t VALUES (99, 'previous')")
			require.NoError(t, err)
			base, err := provider.SnapshotSink(middlewares.Config{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			sink := base.(*snapshot.Sink)
			cleanupKind := abstract.DropTableKind
			if policy == model.Truncate {
				cleanupKind = abstract.TruncateTableKind
			}
			// UploadTables sends cleanup intent before reading the source schema.
			require.NoError(t, sink.Push([]abstract.ChangeItem{{Kind: cleanupKind, Schema: "public", Table: "t"}}))
			var previousID int64
			require.NoError(t, db.QueryRow("SELECT id FROM "+destination.Database+".t").Scan(&previousID))
			require.Equal(t, int64(99), previousID, "cleanup must wait for snapshot publication")
			schema := abstract.NewTableSchema([]abstract.ColSchema{
				{ColumnName: "id", DataType: "int64", PrimaryKey: true, Required: true},
				{ColumnName: "value", DataType: "utf8", Required: true},
			})
			item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t", PartID: "part", TableSchema: schema}
			require.NoError(t, sink.Push([]abstract.ChangeItem{item}))
			item.Kind = abstract.InitTableLoad
			require.NoError(t, sink.Push([]abstract.ChangeItem{item}))
			item.Kind = abstract.InsertKind
			item.ColumnNames = []string{"id", "value"}
			item.ColumnValues = []any{int64(7), "new"}
			require.NoError(t, sink.Push([]abstract.ChangeItem{item}))
			item.Kind = abstract.DoneTableLoad
			require.NoError(t, sink.Push([]abstract.ChangeItem{item}))
			require.NoError(t, sink.Commit(ctx))
			var storedIDs []int64
			require.NoError(t, db.QueryRow("SELECT arraySort(groupArray(id)) FROM "+destination.Database+".t").Scan(&storedIDs))
			require.Equal(t, []int64{7}, storedIDs)
			stateKeys, err := provider.cp.GetTransferStateKeys(provider.transfer.ID)
			require.NoError(t, err)
			require.Empty(t, stateKeys, "successful partial snapshots must leave no EO state")
		})
	}
}

func TestSnapshotAdapterNativeReplacing(t *testing.T) {
	provider, db := newSnapshotAdapterNativeProvider(t, []model.AltName{{From: "t", To: "events-live"}})
	database := provider.transfer.Dst.(*chmodel.ChDestination).Database
	_, err := db.Exec("CREATE TABLE " + database + ".events_live (id Int64 COMMENT 'daily TTL report {table} {database}', value String) ENGINE=MergeTree ORDER BY id")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO " + database + ".events_live VALUES (99, 'previous')")
	require.NoError(t, err)
	base, err := provider.SnapshotSink(middlewares.Config{})
	require.NoError(t, err)
	defer base.Close()
	sink := base.(*snapshot.Sink)
	cols := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "int64", PrimaryKey: true, Required: true}, {ColumnName: "value", DataType: "utf8", Required: true}})
	control := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t", TableSchema: cols}
	require.NoError(t, sink.Push([]abstract.ChangeItem{control}))
	state := transfer_state.NewState(provider.cp, provider.transfer.ID, provider.operation.OperationID)
	stateKeys, err := provider.cp.GetTransferStateKeys(provider.transfer.ID)
	require.NoError(t, err)
	require.Equal(t, []string{state.TableStateKey(control.TableID())}, stateKeys)
	control.Kind = abstract.InitTableLoad
	require.NoError(t, sink.Push([]abstract.ChangeItem{control}))
	require.NoError(t, sink.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, Schema: "public", Table: "t", TableSchema: cols, ColumnNames: []string{"id", "value"}, ColumnValues: []interface{}{int64(7), "value"}, CommitTime: 123}}))
	control.Kind = abstract.DoneTableLoad
	require.NoError(t, sink.Push([]abstract.ChangeItem{control}))
	require.NoError(t, sink.Commit(context.Background()))
	var id int64
	var value string
	var version, deleted uint64
	require.NoError(t, db.QueryRow("SELECT id, value, __data_transfer_commit_time, __data_transfer_delete_time FROM "+database+".events_live FINAL").Scan(&id, &value, &version, &deleted))
	require.Equal(t, int64(7), id)
	require.Equal(t, "value", value)
	require.Equal(t, uint64(123), version)
	require.Zero(t, deleted)
	stateKeys, err = provider.cp.GetTransferStateKeys(provider.transfer.ID)
	require.NoError(t, err)
	require.Empty(t, stateKeys, "successful snapshots must leave no EO state")
	var rawAliasTables uint64
	require.NoError(t, db.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND name = ?", database, "events-live").Scan(&rawAliasTables))
	require.Zero(t, rawAliasTables)
}

func TestSnapshotAdapterRejectsNormalizedAlternativeNameCollision(t *testing.T) {
	provider, _ := newSnapshotAdapterNativeProvider(t, []model.AltName{{From: "first", To: "a-b"}, {From: "second", To: "a_b"}})
	base, err := provider.SnapshotSink(middlewares.Config{})
	require.NoError(t, err)
	defer base.Close()
	columns := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "id", DataType: "int64", PrimaryKey: true, Required: true}})
	require.NoError(t, base.Push([]abstract.ChangeItem{{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "first", TableSchema: columns}}))
	err = base.Push([]abstract.ChangeItem{{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "second", TableSchema: columns}})
	require.ErrorContains(t, err, "map to the same destination a_b")
	state := transfer_state.NewState(provider.cp, provider.transfer.ID, provider.operation.OperationID)
	groups, err := state.ListTableStateKeysByOperation(context.Background())
	require.NoError(t, err)
	require.Len(t, groups[provider.operation.OperationID], 1)
}
