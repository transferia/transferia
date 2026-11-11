package snapshot

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/blang/semver/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/part_uploader"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/transfer_state"
	"github.com/transferia/transferia/pkg/providers/metrika/ddlbuilder"
	"go.ytsaurus.tech/library/go/core/log"
)

func TestPushRetainsFirstError(test *testing.T) {
	// This path must reject rows before touching any connection or callback.
	sink, err := newTestSink(
		logger.Log,
		&clickhouse_model.ChDestination{Database: "test", Cleanup: model.Drop},
		newSerialState(),
		"transfer",
		"operation",
		[][]ch_eo_common.Node{{ch_eo_common.NewNode(&conn_clickhouse.Host{Name: "unused"}, new(sql.DB), newLostHTTPReply(nil, func(statement string) bool { return strings.HasPrefix(statement, "EXCHANGE TABLES") }))}},
		false,
		sharding.ConstSharder(),
		nil,
		nil,
		nil,
	)
	require.NoError(test, err)
	require.NoError(test, sink.Push(nil))

	firstErr := sink.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, Table: "table", PartID: "part"}})
	require.ErrorContains(test, firstErr, "rows outside an open snapshot attempt")
	var uploadErr *abstract.TableUploadError
	require.ErrorAs(test, firstErr, &uploadErr)
	require.Equal(test, firstErr, sink.Push(nil))
	require.Equal(test, firstErr, sink.Push([]abstract.ChangeItem{{Kind: abstract.SynchronizeKind}}))
}

func TestInitAttemptRequiresInitializedTable(test *testing.T) {
	sink, err := newTestSink(
		logger.Log,
		&clickhouse_model.ChDestination{Database: "test", Cleanup: model.Drop},
		newSerialState(),
		"transfer",
		"operation",
		[][]ch_eo_common.Node{{ch_eo_common.NewNode(&conn_clickhouse.Host{Name: "unused"}, new(sql.DB), newLostHTTPReply(nil, func(statement string) bool { return strings.HasPrefix(statement, "EXCHANGE TABLES") }))}},
		false,
		sharding.ConstSharder(),
		nil,
		nil,
		nil,
	)
	require.NoError(test, err)
	item := abstract.ChangeItem{Kind: abstract.InitTableLoad, Schema: "public", Table: "table", PartID: "part"}
	require.ErrorContains(test, sink.Push([]abstract.ChangeItem{item}), "was not initialized before InitTableLoad")
	keys, err := testStateClient(sink).GetTransferStateKeys("transfer")
	require.NoError(test, err)
	require.Empty(test, keys)
}

func newIntegrationSink(test *testing.T, policy model.CleanupType, replacing bool) *Sink {
	test.Helper()
	if os.Getenv("DT_SNAPSHOT_CLICKHOUSE_TEST") != "1" {
		test.Skip("requires a local ClickHouse on 127.0.0.1:19000/18123")
	}
	dst := clickhouse_model.ChDestination{Database: "default", User: "default", NativePort: 19000, HTTPPort: 18123, Cleanup: policy}
	if port := os.Getenv("DT_SNAPSHOT_CLICKHOUSE_NATIVE_PORT"); port != "" {
		parsedPort, err := strconv.Atoi(port)
		require.NoError(test, err)
		dst.NativePort = parsedPort
	}
	if port := os.Getenv("DT_SNAPSHOT_CLICKHOUSE_HTTP_PORT"); port != "" {
		parsedPort, err := strconv.Atoi(port)
		require.NoError(test, err)
		dst.HTTPPort = parsedPort
	}
	params, err := dst.ToSinkParams(&model.Transfer{})
	require.NoError(test, err)
	host := &conn_clickhouse.Host{Name: "127.0.0.1", NativePort: dst.NativePort, HTTPPort: dst.HTTPPort}
	db, err := conn.ConnectNative(host, params)
	require.NoError(test, err)
	database := "dt_snapshot_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = db.Exec("CREATE DATABASE " + ch_eo_common.QuoteIdentifier(database) + " ENGINE = Atomic")
	require.NoError(test, err)
	test.Cleanup(
		func() {
			_, err := db.Exec("DROP DATABASE " + ch_eo_common.QuoteIdentifier(database) + " SYNC")
			require.NoError(test, err)
			require.NoError(test, db.Close())
		},
	)
	dst.Database = database
	params, err = dst.ToSinkParams(&model.Transfer{})
	require.NoError(test, err)
	cl, err := httpclient.NewHTTPClientImpl(params)
	require.NoError(test, err)
	engine := "MergeTree()"
	if replacing {
		engine = "ReplacingMergeTree(version)"
	}
	sink, err := newTestSink(
		logger.Log,
		&dst,
		newSerialState(),
		"transfer",
		"dtj"+strings.ReplaceAll(uuid.NewString(), "-", "")[:17],
		[][]ch_eo_common.Node{{ch_eo_common.NewNode(host, db, newLostHTTPReply(cl, func(statement string) bool { return strings.HasPrefix(statement, "EXCHANGE TABLES") }))}},
		false,
		sharding.ConstSharder(),
		func(item abstract.ChangeItem) string { return item.Table },
		func(abstract.ChangeItem) (string, error) {
			return "CREATE TABLE t (id UInt64, version UInt64) ENGINE = " + engine + " ORDER BY id", nil
		},
		func(node ch_eo_common.Node, table string, rows []abstract.ChangeItem) error {
			var values []string
			for _, row := range rows {
				values = append(values, fmt.Sprintf("(%d,%d)", row.ColumnValues[0], row.ColumnValues[1]))
			}
			_, err := node.DB.Exec("INSERT INTO " + ch_eo_common.QuoteIdentifier(database) + "." + ch_eo_common.QuoteIdentifier(table) + " VALUES " + strings.Join(values, ","))
			return err
		},
	)
	require.NoError(test, err)
	require.NoError(test, sink.initDatabaseOperations(context.Background()))
	test.Cleanup(func() {
		sink.stopStagingDDLKeepalives()
		require.NoError(test, sink.uploader.Close())
		testUploadBySink.Delete(sink)
	})
	return sink
}

func newIntegrationWorker(test *testing.T, source *Sink, operationID string) *Sink {
	test.Helper()
	if operationID != source.publicationStore.OperationID() {
		source.stopStagingDDLKeepalives()
		require.NoError(test, source.uploader.Close())
	}
	sink, err := newTestSink(
		source.logger,
		source.dstConfig,
		testStateClient(source),
		source.transferID,
		operationID,
		source.tablesClient.NodesByShard(),
		source.defaultReplicatedTableEngine,
		testUploaderFixture(source).sharder,
		source.destinationTableNameFunc,
		source.destinationDDLFunc,
		testWriteRows(source),
	)
	require.NoError(test, err)
	require.NoError(test, sink.initDatabaseOperations(context.Background()))
	test.Cleanup(func() {
		sink.stopStagingDDLKeepalives()
		require.NoError(test, sink.uploader.Close())
		testUploadBySink.Delete(sink)
	})
	return sink
}

func loadSnapshotTable(test *testing.T, sink *Sink, tableID abstract.TableID) *transfer_state.TableState {
	test.Helper()
	table, err := sink.publicationStore.LoadTable(context.Background(), sink.publicationStore.TableStateKey(tableID))
	require.NoError(test, err)
	require.NotNil(test, table, "snapshot table %s must be initialized", tableID.String())
	return table
}
func defaultSnapshotTable(test *testing.T, sink *Sink) *transfer_state.TableState {
	test.Helper()
	keys, err := sink.publicationStore.ListStateKeysWithPrefix(context.Background(), sink.publicationStore.OperationStatePrefix()+"table/")
	require.NoError(test, err)
	require.Len(test, keys, 1)
	table, err := sink.publicationStore.LoadTable(context.Background(), keys[0])
	require.NoError(test, err)
	require.NotNil(test, table)
	return table
}
func readyParts(test *testing.T, sink *Sink, table *transfer_state.TableState) []ch_eo_common.ReadyPart {
	test.Helper()
	require.NoError(test, sink.initDatabaseOperations(context.Background()))
	parts, err := part_uploader.DiscoverReady(context.Background(), sink.tablesClient, sink.operationsForTable(table), sink.dstConfig.Database, table.Snapshot())
	require.NoError(test, err)
	return parts
}
func buildTestCandidates(test *testing.T, sink *Sink, table *transfer_state.TableState) error {
	test.Helper()
	parts := readyParts(test, sink, table)
	return sink.buildCandidateTable(context.Background(), table, parts)
}

func TestIntegrationSnapshot(test *testing.T) {
	for _, policy := range []model.CleanupType{model.Drop, model.Truncate, model.DisabledCleanup} {
		for _, replacing := range []bool{false, true} {
			test.Run(fmt.Sprintf("%s/replacing=%t", policy, replacing), func(test *testing.T) {
				sink := newIntegrationSink(test, policy, replacing)
				ctx := context.Background()
				ddl, err := sink.destinationDDLFunc(abstract.ChangeItem{})
				require.NoError(test, err)
				ddl, err = ch_eo_common.BuildCreateTableDDL(sink.dstConfig.Database, "t", uuid.NewString(), ddl, "")
				require.NoError(test, err)
				_, err = sink.tablesClient.NodesByShard()[0][0].DB.Exec(ddl)
				require.NoError(test, err)
				_, err = sink.tablesClient.NodesByShard()[0][0].DB.Exec("INSERT INTO " + sink.tablesClient.QualifiedTableName("t") + " VALUES (10,1)")
				require.NoError(test, err)
				init := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
				require.NoError(test, sink.Push([]abstract.ChangeItem{init}))
				init.Kind = abstract.InitTableLoad
				require.NoError(test, sink.Push([]abstract.ChangeItem{init}))
				row := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: "public", Table: "t", ColumnValues: []interface{}{uint64(1), uint64(2)}}
				require.NoError(test, sink.Push([]abstract.ChangeItem{row}))
				init.Kind = abstract.DoneTableLoad
				require.NoError(test, sink.Push([]abstract.ChangeItem{init}))
				count, err := sink.tablesClient.CountPhysicalRows(ctx, "127.0.0.1", "t")
				require.NoError(test, err)
				require.Equal(test, uint64(1), count)
				require.NoError(test, sink.Commit(context.Background()))
				count, err = sink.tablesClient.CountPhysicalRows(ctx, "127.0.0.1", "t")
				require.NoError(test, err)
				expected := uint64(1)
				if policy == model.DisabledCleanup {
					expected++
				}
				require.Equal(test, expected, count)
				keys, err := testStateClient(sink).GetTransferStateKeys(sink.transferID)
				require.NoError(test, err)
				require.Empty(test, keys)
				var resources uint64
				require.NoError(test, sink.tablesClient.NodesByShard()[0][0].DB.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__dt_')", sink.dstConfig.Database).Scan(&resources))
				require.Zero(test, resources)
				require.True(test, sink.tablesClient.NodesByShard()[0][0].HTTP.(*lostHTTPReply).lost)
			})
		}
	}
}

func TestIntegrationReplicatedCandidate(test *testing.T) {
	if os.Getenv("DT_SNAPSHOT_CLICKHOUSE_REPLICAS_TEST") != "1" {
		test.Skip("requires two ClickHouse replicas with a shared Keeper")
	}
	for _, scenario := range []string{"merge_tree", "replacing_merge_tree", "unavailable_replica", "replica_unavailable_at_commit"} {
		test.Run(scenario, func(test *testing.T) {
			replacing := scenario == "replacing_merge_tree"
			sink := newIntegrationSink(test, model.DisabledCleanup, replacing)
			sink.transferID = "dtt" + strings.ReplaceAll(uuid.NewString(), "-", "")[:17]
			sink.publicationStore = transfer_state.NewState(testStateClient(sink), sink.transferID, sink.publicationStore.OperationID())
			sink.defaultReplicatedTableEngine = true
			dst := clickhouse_model.ChDestination{Database: "default", User: "default", NativePort: 19001, HTTPPort: 18124}
			params, err := dst.ToSinkParams(&model.Transfer{})
			require.NoError(test, err)
			host := &conn_clickhouse.Host{Name: "localhost", NativePort: 19001, HTTPPort: 18124}
			db, err := conn.ConnectNative(host, params)
			require.NoError(test, err)
			_, err = db.Exec("CREATE DATABASE " + ch_eo_common.QuoteIdentifier(sink.dstConfig.Database) + " ENGINE = Atomic")
			require.NoError(test, err)
			dst.Database = sink.dstConfig.Database
			params, err = dst.ToSinkParams(&model.Transfer{})
			require.NoError(test, err)
			test.Cleanup(
				func() {
					_, err := db.Exec("DROP DATABASE " + ch_eo_common.QuoteIdentifier(sink.dstConfig.Database) + " SYNC")
					require.NoError(test, err)
					require.NoError(test, db.Close())
				},
			)
			cl, err := httpclient.NewHTTPClientImpl(params)
			require.NoError(test, err)
			configureTestNodes(test, sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
				nodes[0] = append(nodes[0], ch_eo_common.NewNode(host, db, cl))

				return nodes
			},
			)
			sink = newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
			engine := "ReplicatedMergeTree"
			if replacing {
				engine = "ReplicatedReplacingMergeTree"
			}
			for replicaIndex, node := range sink.tablesClient.NodesByShard()[0] {
				args := ch_eo_common.QuoteStringLiteral("/dt_snapshot_tests/"+sink.dstConfig.Database) + ", " + ch_eo_common.QuoteStringLiteral(fmt.Sprint(replicaIndex))
				if replacing {
					args += ", version"
				}
				_, err := node.DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE = " + engine + "(" + args + ") ORDER BY id")
				require.NoError(test, err)
			}
			_, err = sink.tablesClient.NodesByShard()[0][0].DB.Exec("INSERT INTO " + sink.tablesClient.QualifiedTableName("t") + " VALUES (1,1)")
			require.NoError(test, err)
			var unavailableReplica *unavailableHTTPClient
			if scenario == "unavailable_replica" || scenario == "replica_unavailable_at_commit" {
				client := newUnavailableHTTPClient(sink.tablesClient.NodesByShard()[0][1].HTTP)
				unavailableReplica = client
				client.unavailable.Store(scenario == "unavailable_replica")
				defer client.unavailable.Store(false)
				configureTestNodes(test, sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
					nodes[0][1].HTTP = client
					return nodes
				})
				sink.tablesClient.SetReplicaFailureGrace(0)
			}
			item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			item.Kind = abstract.InitTableLoad
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			version := uint64(1)
			if replacing {
				version = 2
			}
			// For MergeTree old and new parts have identical checksums. They are
			// two legitimate copies and must survive replicated import together.
			require.NoError(test, sink.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: "public", Table: "t", ColumnValues: []interface{}{uint64(1), version}}}))
			item.Kind = abstract.DoneTableLoad
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			if unavailableReplica != nil {
				unavailableReplica.unavailable.Store(true)
			}
			require.NoError(test, sink.Commit(context.Background()))
			for nodeIndex, node := range sink.tablesClient.NodesByShard()[0] {
				var count, actualVersion uint64
				query := "SELECT count(), max(version) FROM " + sink.tablesClient.QualifiedTableName("t")
				if replacing {
					query += " FINAL"
				}
				require.NoError(test, node.DB.QueryRow(query).Scan(&count, &actualVersion))
				if unavailableReplica != nil && nodeIndex == 1 {
					require.EqualValues(test, 1, count, "skipped Atomic replica keeps its old table")
					require.EqualValues(test, 1, actualVersion)
					continue
				}
				if replacing {
					require.Equal(test, uint64(1), count)
				} else {
					require.Equal(test, uint64(2), count)
				}
				require.Equal(test, version, actualVersion)
				var resources uint64
				require.NoError(test, node.DB.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__dt_')", sink.dstConfig.Database).Scan(&resources))
				require.Zero(test, resources)
			}
			for _, node := range sink.tablesClient.NodesByShard()[0] {
				_, err := node.DB.Exec("DROP TABLE " + sink.tablesClient.QualifiedTableName("t") + " SYNC")
				require.NoError(test, err)
			}
			var keeperNodes uint64
			require.NoError(test, sink.tablesClient.NodesByShard()[0][0].DB.QueryRow("SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/__dt_snapshot' AND startsWith(name, ?)", sink.transferID+"_").Scan(&keeperNodes))
			require.Zero(test, keeperNodes, "dropping the last generation must not leave empty Keeper parents")
		})
	}
}

func TestIntegrationPoisonedAttempt(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, false)
	init := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{init}))
	init.Kind = abstract.InitTableLoad
	write := testWriteRows(sink)
	writes := 0
	setTestAppendRows(test, sink, func(node ch_eo_common.Node, table string, rows []abstract.ChangeItem) error {
		writes++
		if err := write(node, table, rows); err != nil {
			return err
		}
		return xerrors.New("lost INSERT response")
	},
	)
	require.NoError(test, sink.Push([]abstract.ChangeItem{init}))
	row := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: "public", Table: "t", ColumnValues: []interface{}{uint64(1), uint64(2)}}
	require.Error(test, sink.Push([]abstract.ChangeItem{row}))
	require.Error(test, sink.Push([]abstract.ChangeItem{row}))
	require.Equal(test, 1, writes)
	// A new whole-part attempt writes to a new table and is the only selected one.
	retry := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
	setTestAppendRows(test, retry, write)
	require.NoError(test, retry.Push([]abstract.ChangeItem{init, row}))
	init.Kind = abstract.DoneTableLoad
	require.NoError(test, retry.Push([]abstract.ChangeItem{init}))
	require.NoError(test, retry.Commit(context.Background()))
	count, err := retry.tablesClient.CountPhysicalRows(context.Background(), "127.0.0.1", "t")
	require.NoError(test, err)
	require.Equal(test, uint64(1), count)
}

func TestIntegrationCleanupFinishesPreparedPublication(test *testing.T) {
	for _, executed := range []bool{false, true} {
		test.Run(fmt.Sprint(executed), func(test *testing.T) {
			sink := newIntegrationSink(test, model.DisabledCleanup, false)
			ctx := context.Background()
			_, err := sink.tablesClient.NodesByShard()[0][0].DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE = MergeTree ORDER BY id")
			require.NoError(test, err)
			_, err = sink.tablesClient.NodesByShard()[0][0].DB.Exec("INSERT INTO " + sink.tablesClient.QualifiedTableName("t") + " VALUES (9,1)")
			require.NoError(test, err)
			item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			item.Kind = abstract.InitTableLoad
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			row := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: "public", Table: "t", ColumnValues: []interface{}{uint64(1), uint64(2)}}
			require.NoError(test, sink.Push([]abstract.ChangeItem{row}))
			item.Kind = abstract.DoneTableLoad
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			record := loadSnapshotTable(test, sink, item.TableID())
			require.NoError(test, buildTestCandidates(test, sink, record))
			require.NoError(test, record.DecidePublication(ctx))
			node := record.Replicas()[0]
			command, err := sink.tablesClient.PrepareSessionCommand(ctx, node.HostName, "EXCHANGE TABLES "+sink.tablesClient.QualifiedTableName(record.DestinationTableName())+" AND "+sink.tablesClient.QualifiedTableName(node.CandidateTable.TableName))
			require.NoError(test, err)
			require.NoError(test, record.RecordPublicationCommand(ctx, node.HostName, command))
			if executed {
				require.Error(test, sink.tablesClient.ExecuteCommandOverHTTP(ctx, node.HostName, command.SQL, ch_eo_common.ExistingSessionParams(command.SessionID)))
			}
			// A different cleanup process reconstructs everything from serialized state.
			require.NoError(test, testStateClient(sink).SetTransferState(sink.transferID, map[string]*coordinator.TransferStateData{"cdc_offset": {Generic: "keep"}}))
			recovery := newIntegrationWorker(test, sink, "cleanup-operation")
			require.NoError(test, recovery.Cleanup(ctx))
			// The old request was delayed past worker teardown. It must be rejected,
			// not swap the live name back after cleanup has reported success.
			require.Error(test, recovery.tablesClient.ExecuteCommandOverHTTP(ctx, node.HostName, command.SQL, ch_eo_common.ExistingSessionParams(command.SessionID)))
			count, err := recovery.tablesClient.CountPhysicalRows(ctx, node.HostName, "t")
			require.NoError(test, err)
			require.Equal(test, uint64(2), count)
			keys, err := testStateClient(sink).GetTransferStateKeys(sink.transferID)
			require.NoError(test, err)
			require.Equal(test, []string{"cdc_offset"}, keys)
		})
	}
}

func TestIntegrationCleanupFencesDelayedCreate(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, false)
	ctx := context.Background()
	require.NoError(test, sink.Push([]abstract.ChangeItem{{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}}))
	resource := ch_eo_common.NewTemporaryTable(defaultSnapshotTable(test, sink).TemporaryTablePrefix(), "127.0.0.1", ch_eo_common.RoleAssembly)
	command := ch_eo_common.NewCommand(defaultSnapshotTable(test, sink).StagingSessions()[resource.CommandHostName], "CREATE TABLE "+sink.tablesClient.QualifiedTableName(resource.TableName)+" UUID "+ch_eo_common.QuoteStringLiteral(resource.ExpectedTableUUID)+" (id UInt64) ENGINE=MergeTree ORDER BY id")
	sink.stopStagingDDLKeepalives()
	recovery := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
	require.NoError(test, recovery.Cleanup(ctx))
	require.Error(test, recovery.tablesClient.ExecuteCommandOverHTTP(ctx, resource.CommandHostName, command.SQL, ch_eo_common.ExistingSessionParams(command.SessionID)))
	id, err := recovery.tablesClient.TableUUID(ctx, resource.CommandHostName, resource.TableName)
	require.NoError(test, err)
	require.Empty(test, id)
}

func TestIntegrationCleanupPreservesPreparedOldGenerationDrop(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, false)
	ctx := context.Background()
	_, err := sink.tablesClient.NodesByShard()[0][0].DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE = MergeTree ORDER BY id")
	require.NoError(test, err)
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	record := loadSnapshotTable(test, sink, item.TableID())
	require.NoError(test, buildTestCandidates(test, sink, record))
	require.NoError(test, record.DecidePublication(ctx))
	// The wrapper loses the first EXCHANGE response; reconcile its UUID next.
	require.Error(test, sink.operationsForTable(record).Publish(ctx))
	require.NoError(test, sink.operationsForTable(record).Publish(ctx))
	resource := record.Replicas()[0].CandidateTable
	resource.ExpectedTableUUID = record.Replicas()[0].PreviousTableUUID
	command, err := sink.tablesClient.PrepareSessionCommand(ctx, resource.CommandHostName, "DROP TABLE "+sink.tablesClient.QualifiedTableName(resource.TableName)+" SYNC")
	require.NoError(test, err)
	resource.PendingCommand = command
	require.NoError(test, record.RecordPendingTableCommand(ctx, &resource, command))
	// The old worker has persisted but not dispatched this DROP.
	recovery := newIntegrationWorker(test, sink, "cleanup-operation")
	require.NoError(test, recovery.Cleanup(ctx))
	err = recovery.tablesClient.ExecuteCommandOverHTTP(ctx, resource.CommandHostName, command.SQL, ch_eo_common.ExistingSessionParams(command.SessionID))
	var exception *clickhouse_go.Exception
	require.ErrorAs(test, err, &exception)
	require.EqualValues(test, sessionNotFoundCode, exception.Code)
	id, err := recovery.tablesClient.TableUUID(ctx, resource.CommandHostName, "t")
	require.NoError(test, err)
	require.Equal(test, record.Replicas()[0].CandidateTable.ExpectedTableUUID, id)
}

func TestIntegrationEmptySnapshotAndIncompleteAttempt(test *testing.T) {
	for _, incomplete := range []bool{false, true} {
		test.Run(fmt.Sprint(incomplete), func(test *testing.T) {
			sink := newIntegrationSink(test, model.Truncate, false)
			ctx := context.Background()
			_, err := sink.tablesClient.NodesByShard()[0][0].DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE = MergeTree ORDER BY id")
			require.NoError(test, err)
			_, err = sink.tablesClient.NodesByShard()[0][0].DB.Exec("INSERT INTO " + sink.tablesClient.QualifiedTableName("t") + " VALUES (9,1)")
			require.NoError(test, err)
			oldID, err := sink.tablesClient.TableUUID(ctx, "127.0.0.1", "t")
			require.NoError(test, err)
			item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			if incomplete {
				item.Kind = abstract.InitTableLoad
				require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			}
			err = sink.Commit(context.Background())
			if incomplete {
				require.Error(test, err)
				require.NoError(test, sink.Cleanup(ctx))
				id, err := sink.tablesClient.TableUUID(ctx, "127.0.0.1", "t")
				require.NoError(test, err)
				require.Equal(test, oldID, id)
			} else {
				require.NoError(test, err)
			}
			rows, err := sink.tablesClient.CountPhysicalRows(ctx, "127.0.0.1", "t")
			require.NoError(test, err)
			expected := uint64(0)
			if incomplete {
				expected = 1
			}
			require.Equal(test, expected, rows)
			keys, err := testStateClient(sink).GetTransferStateKeys(sink.transferID)
			require.NoError(test, err)
			require.Empty(test, keys)
		})
	}
}

func TestIntegrationCancelledPublisherHandsOffToCleanup(test *testing.T) {
	sink := newIntegrationSink(test, model.Truncate, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configureTestNodes(test, sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP.(*lostHTTPReply).cancel = cancel

		return nodes
	},
	)
	_, err := sink.tablesClient.NodesByShard()[0][0].DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE = MergeTree ORDER BY id")
	require.NoError(test, err)
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	require.ErrorIs(test, sink.Commit(ctx), context.Canceled)
	record, err := sink.publicationStore.LoadTable(context.Background(), sink.publicationStore.TableStateKey(item.TableID()))
	require.NoError(test, err)
	require.NotNil(test, record)
	require.True(test, record.PublicationDecided())
	require.False(test, record.PublicationComplete())
	recovery := newIntegrationWorker(test, sink, "cleanup-operation")
	require.NoError(test, recovery.Cleanup(context.Background()))
	id, err := recovery.tablesClient.TableUUID(context.Background(), "127.0.0.1", "t")
	require.NoError(test, err)
	require.Equal(test, record.Replicas()[0].CandidateTable.ExpectedTableUUID, id)
}

func TestIntegrationCleanupWithoutParts(test *testing.T) {
	for _, policy := range []model.CleanupType{model.Drop, model.Truncate, model.DisabledCleanup} {
		test.Run(string(policy), func(test *testing.T) {
			sink := newIntegrationSink(test, policy, false)
			ctx := context.Background()
			_, err := sink.tablesClient.NodesByShard()[0][0].DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE = MergeTree ORDER BY id")
			require.NoError(test, err)
			_, err = sink.tablesClient.NodesByShard()[0][0].DB.Exec("INSERT INTO " + sink.tablesClient.QualifiedTableName("t") + " VALUES (9,1)")
			require.NoError(test, err)
			oldID, err := sink.tablesClient.TableUUID(ctx, "127.0.0.1", "t")
			require.NoError(test, err)
			if policy != model.DisabledCleanup {
				kind := abstract.DropTableKind
				if policy == model.Truncate {
					kind = abstract.TruncateTableKind
				}
				item := abstract.ChangeItem{Kind: kind, Schema: "public", Table: "t"}
				require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
				// A lost cleanup ACK may cause the same intent to be delivered twice.
				require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			}
			rows, err := sink.tablesClient.CountPhysicalRows(ctx, "127.0.0.1", "t")
			require.NoError(test, err)
			require.EqualValues(test, 1, rows, "cleanup intent must preserve live data until Commit")
			if policy != model.DisabledCleanup {
				// The loader sends the source schema even when the table has no parts.
				require.NoError(test, sink.Push([]abstract.ChangeItem{{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t", TableSchema: snapshotTestSchema}}))
			}
			publisher := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
			require.NoError(test, publisher.Commit(context.Background()))
			rows, err = publisher.tablesClient.CountPhysicalRows(ctx, "127.0.0.1", "t")
			require.NoError(test, err)
			id, err := publisher.tablesClient.TableUUID(ctx, "127.0.0.1", "t")
			require.NoError(test, err)
			if policy == model.DisabledCleanup {
				require.EqualValues(test, 1, rows)
				require.Equal(test, oldID, id)
			} else {
				require.Zero(test, rows)
				require.NotEqual(test, oldID, id)
			}
			keys, err := testStateClient(sink).GetTransferStateKeys(sink.transferID)
			require.NoError(test, err)
			require.Empty(test, keys)
		})
	}
}

func TestIntegrationDisabledWithoutPartsDoesNotCreateTable(test *testing.T) {
	sink := newIntegrationSink(test, model.DisabledCleanup, false)
	require.NoError(test, sink.Commit(context.Background()))
	id, err := sink.tablesClient.TableUUID(context.Background(), "127.0.0.1", "t")
	require.NoError(test, err)
	require.Empty(test, id)
}

func TestIntegrationPartsAfterMainInitialization(test *testing.T) {
	sink := newIntegrationSink(test, model.DisabledCleanup, false)
	init := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{init}))
	expectedTableState := loadSnapshotTable(test, sink, init.TableID())
	for _, partID := range []string{"a", "b"} {
		worker := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
		item := abstract.ChangeItem{Kind: abstract.InitTableLoad, Schema: "public", Table: "t", PartID: partID}
		require.NoError(test, worker.Push([]abstract.ChangeItem{item}))
		row := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: "public", Table: "t", PartID: partID, ColumnValues: []interface{}{uint64(1), uint64(1)}}
		require.NoError(test, worker.Push([]abstract.ChangeItem{row}))
		item.Kind = abstract.DoneTableLoad
		require.NoError(test, worker.Push([]abstract.ChangeItem{item}))
		definition, err := worker.publicationStore.LoadTable(context.Background(), worker.publicationStore.TableStateKey(item.TableID()))
		require.NoError(test, err)
		require.NotNil(test, definition)
		require.Equal(test, expectedTableState.Replicas(), definition.Replicas())
		require.Equal(test, expectedTableState.StagingSessions(), definition.StagingSessions())
		require.False(test, definition.PublicationDecided())
	}
	publisher := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
	require.NoError(test, publisher.Commit(context.Background()))
	rows, err := publisher.tablesClient.CountPhysicalRows(context.Background(), "127.0.0.1", "t")
	require.NoError(test, err)
	require.EqualValues(test, 2, rows)
	keys, err := testStateClient(sink).GetTransferStateKeys(sink.transferID)
	require.NoError(test, err)
	require.Empty(test, keys)
}

func TestIntegrationResetDiscardsPreparedPublication(test *testing.T) {
	for _, executed := range []bool{false, true} {
		test.Run(fmt.Sprint(executed), func(test *testing.T) {
			sink := newIntegrationSink(test, model.DisabledCleanup, false)
			ctx := context.Background()
			_, err := sink.tablesClient.NodesByShard()[0][0].DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE = MergeTree ORDER BY id")
			require.NoError(test, err)
			_, err = sink.tablesClient.NodesByShard()[0][0].DB.Exec("INSERT INTO " + sink.tablesClient.QualifiedTableName("t") + " VALUES (9,1)")
			require.NoError(test, err)
			item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			item.Kind = abstract.InitTableLoad
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			row := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: "public", Table: "t", ColumnValues: []interface{}{uint64(1), uint64(2)}}
			require.NoError(test, sink.Push([]abstract.ChangeItem{row}))
			item.Kind = abstract.DoneTableLoad
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			record := loadSnapshotTable(test, sink, item.TableID())
			require.NoError(test, buildTestCandidates(test, sink, record))
			require.NoError(test, record.DecidePublication(ctx))
			node := record.Replicas()[0]
			command, err := sink.tablesClient.PrepareSessionCommand(ctx, node.HostName, "EXCHANGE TABLES "+sink.tablesClient.QualifiedTableName(record.DestinationTableName())+" AND "+sink.tablesClient.QualifiedTableName(node.CandidateTable.TableName))
			require.NoError(test, err)
			require.NoError(test, record.RecordPublicationCommand(ctx, node.HostName, command))
			if executed {
				require.Error(test, sink.tablesClient.ExecuteCommandOverHTTP(ctx, node.HostName, command.SQL, ch_eo_common.ExistingSessionParams(command.SessionID)))
			}
			// A different cleanup process reconstructs everything from serialized state.
			require.NoError(test, testStateClient(sink).SetTransferState(sink.transferID, map[string]*coordinator.TransferStateData{"cdc_offset": {Generic: "keep"}}))
			recovery := newIntegrationWorker(test, sink, "cleanup-operation")
			require.NoError(test, recovery.ResetPreviousOperations(ctx))
			// The old request was delayed past worker teardown. It must be rejected,
			// not swap the live name back after cleanup has reported success.
			require.Error(test, recovery.tablesClient.ExecuteCommandOverHTTP(ctx, node.HostName, command.SQL, ch_eo_common.ExistingSessionParams(command.SessionID)))
			count, err := recovery.tablesClient.CountPhysicalRows(ctx, node.HostName, "t")
			require.NoError(test, err)
			expectedRowCountsByShard := uint64(1)
			if executed {
				expectedRowCountsByShard = 2
			}
			require.Equal(test, expectedRowCountsByShard, count)
			keys, err := testStateClient(sink).GetTransferStateKeys(sink.transferID)
			require.NoError(test, err)
			require.Equal(test, []string{"cdc_offset"}, keys)
		})
	}
}

func TestIntegrationResetPreviousOperationsPreservesCurrentState(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, false)
	ctx := context.Background()
	require.NoError(test, sink.Push([]abstract.ChangeItem{{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}}))
	previous := newIntegrationWorker(test, sink, "previous")
	previousTable := previous.publicationStore.NewTable(abstract.TableID{Namespace: "public", Name: "previous"}, "previous", "", false)
	require.NoError(test, previousTable.RegisterStaging(ctx, "__dt_snapshot_previous_unused_", nil))
	require.NoError(
		test,
		testStateClient(sink).SetTransferState(
			"transfer",
			map[string]*coordinator.TransferStateData{
				"cdc_offset": {Generic: "keep"}, "incremental_cursor": {Generic: "keep"},
			},
		),
	)
	require.NoError(test, sink.ResetPreviousOperations(ctx))
	require.NoError(test, sink.ResetPreviousOperations(ctx))
	keys, err := testStateClient(sink).GetTransferStateKeys("transfer")
	require.NoError(test, err)
	require.ElementsMatch(test, []string{sink.publicationStore.TableStateKey(abstract.TableID{Namespace: "public", Name: "t"}), "cdc_offset", "incremental_cursor"}, keys)
	require.NoError(test, sink.Cleanup(ctx))
}

func newUnitSessionSink(test *testing.T) *Sink {
	test.Helper()
	sink, err := newTestSink(
		logger.Log,
		&clickhouse_model.ChDestination{Database: "test", Cleanup: model.Drop},
		newSerialState(),
		"transfer",
		"operation",
		[][]ch_eo_common.Node{{ch_eo_common.NewNode(&conn_clickhouse.Host{Name: "unused"}, new(sql.DB), newLostHTTPReply(nil, func(statement string) bool { return strings.HasPrefix(statement, "EXCHANGE TABLES") }))}},
		false,
		sharding.ConstSharder(),
		nil,
		nil,
		nil,
	)
	require.NoError(test, err)

	test.Cleanup(func() {
		sink.stopStagingDDLKeepalives()
		require.NoError(test, sink.uploader.Close())
		testUploadBySink.Delete(sink)
	})
	return sink
}

func TestIntegrationRawCountWithReplacing(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, true)
	ctx := context.Background()
	_, err := sink.tablesClient.NodesByShard()[0][0].DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE = ReplacingMergeTree(version) ORDER BY id")
	require.NoError(test, err)
	// Disable insert-time replacement to test physical count versus FINAL.
	_, err = sink.tablesClient.NodesByShard()[0][0].DB.Exec("INSERT INTO " + sink.tablesClient.QualifiedTableName("t") + " SETTINGS optimize_on_insert=0 VALUES (1,1), (1,2)")
	require.NoError(test, err)
	count, err := sink.tablesClient.CountPhysicalRows(ctx, "127.0.0.1", "t")
	require.NoError(test, err)
	require.Equal(test, uint64(2), count)
	var finalCount uint64
	err = sink.tablesClient.NodesByShard()[0][0].DB.QueryRow("SELECT count() FROM " + sink.tablesClient.QualifiedTableName("t") + " FINAL").Scan(&finalCount)
	require.NoError(test, err)
	require.Equal(test, uint64(1), finalCount)
}

func TestIntegrationPartitionImportCompatibility(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, true)
	ctx := context.Background()
	_, err := sink.tablesClient.NodesByShard()[0][0].DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("source") + " (id UInt64, version UInt64) ENGINE = MergeTree ORDER BY id")
	require.NoError(test, err)
	_, err = sink.tablesClient.NodesByShard()[0][0].DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("target") + " (id UInt64, version UInt64) ENGINE = ReplacingMergeTree(version) ORDER BY id")
	require.NoError(test, err)
	_, err = sink.tablesClient.NodesByShard()[0][0].DB.Exec("INSERT INTO " + sink.tablesClient.QualifiedTableName("source") + " VALUES (1,1)")
	require.NoError(test, err)
	query := "ALTER TABLE " + sink.tablesClient.QualifiedTableName("target") + " REPLACE PARTITION ID 'all' FROM " + sink.tablesClient.QualifiedTableName("source")
	err = sink.tablesClient.ExecuteCommandOverHTTP(ctx, "127.0.0.1", query, url.Values{"replication_alter_partitions_sync": {"2"}})
	require.NoError(test, err)
	count, err := sink.tablesClient.CountPhysicalRows(ctx, "127.0.0.1", "target")
	require.NoError(test, err)
	require.Equal(test, uint64(1), count)
}

func TestIntegrationReadyPartReusedByAnotherWorker(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, false)
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t", PartID: "same-part"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	item.Kind = abstract.InitTableLoad
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	row := abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: "public", Table: "t", PartID: item.PartID, ColumnValues: []interface{}{uint64(1), uint64(2)}}
	require.NoError(test, sink.Push([]abstract.ChangeItem{row}))
	item.Kind = abstract.DoneTableLoad
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	worker := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
	setTestAppendRows(test, worker, func(ch_eo_common.Node, string, []abstract.ChangeItem) error {
		return xerrors.New("ready part must not be inserted again")
	},
	)
	item.Kind = abstract.InitTableLoad
	require.NoError(test, worker.Push([]abstract.ChangeItem{item, row}))
	item.Kind = abstract.DoneTableLoad
	require.NoError(test, worker.Push([]abstract.ChangeItem{item}))
	keys, err := testStateClient(sink).GetTransferStateKeys("transfer")
	require.NoError(test, err)
	require.Equal(test, []string{sink.publicationStore.TableStateKey(item.TableID())}, keys)
	require.NoError(test, sink.Commit(context.Background()))
	count, err := sink.tablesClient.CountPhysicalRows(context.Background(), "127.0.0.1", "t")
	require.NoError(test, err)
	require.EqualValues(test, 1, count)
	var remaining uint64
	require.NoError(test, sink.tablesClient.NodesByShard()[0][0].DB.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__dt_')", sink.dstConfig.Database).Scan(&remaining))
	require.Zero(test, remaining)
	keys, err = testStateClient(sink).GetTransferStateKeys("transfer")
	require.NoError(test, err)
	require.Empty(test, keys)
}

func TestIntegrationUnfinishedPartReassignedToAnotherWorker(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, false)
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t", PartID: "shared/part:\"?"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	firstWorker := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
	// Model rows already sent before the unfinished worker loses its final ACK.
	setTestAppendRows(test, firstWorker, testWriteRows(firstWorker))
	item.Kind = abstract.InitTableLoad
	require.NoError(test, firstWorker.Push([]abstract.ChangeItem{item, {Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: item.Schema, Table: item.Table, PartID: item.PartID, ColumnValues: []any{uint64(99), uint64(1)}}}))
	// The scheduler only reassigns a part after its previous writer has stopped.
	firstWorker.stopStagingDDLKeepalives()
	require.NoError(test, firstWorker.uploader.Close())
	secondWorker := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
	require.NoError(test, secondWorker.Push([]abstract.ChangeItem{item, {Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: item.Schema, Table: item.Table, PartID: item.PartID, ColumnValues: []any{uint64(1), uint64(2)}}}))
	item.Kind = abstract.DoneTableLoad
	require.NoError(test, secondWorker.Push([]abstract.ChangeItem{item}))
	ready, err := readReadyAttempt(sink, context.Background(), item.TablePartID())
	require.NoError(test, err)
	require.NotNil(test, ready)
	require.NoError(test, sink.Commit(context.Background()))
	var actualIDs []uint64
	require.NoError(test, sink.tablesClient.NodesByShard()[0][0].DB.QueryRow("SELECT arraySort(groupArray(id)) FROM "+sink.tablesClient.QualifiedTableName("t")).Scan(&actualIDs))
	require.Equal(test, []uint64{1}, actualIDs, "the incomplete attempt must be discarded before rewriting its fixed loading table")
}

func TestIntegrationLostPublicationStateReply(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, false)
	injection := newLostPublicationStateReply(testStateClient(sink))
	testStorageBySink.Store(sink, injection)
	sink.publicationStore = transfer_state.NewState(injection, sink.transferID, sink.publicationStore.OperationID())
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t", PartID: "part"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	item.Kind = abstract.InitTableLoad
	require.NoError(test, sink.Push([]abstract.ChangeItem{item, {Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: item.Schema, Table: item.Table, PartID: item.PartID, ColumnValues: []interface{}{uint64(1), uint64(2)}}}))
	item.Kind = abstract.DoneTableLoad
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	require.ErrorContains(test, sink.Commit(context.Background()), "injected lost publication state reply")
	require.True(test, injection.lost)
	recovery := newIntegrationWorker(test, sink, "cleanup-operation")
	require.NoError(test, recovery.Cleanup(context.Background()))
	count, err := recovery.tablesClient.CountPhysicalRows(context.Background(), "127.0.0.1", "t")
	require.NoError(test, err)
	require.EqualValues(test, 1, count)
	keys, err := testStateClient(sink).GetTransferStateKeys("transfer")
	require.NoError(test, err)
	require.Empty(test, keys)
}

func TestCommitWithoutTableEventsNeedsNoDestination(test *testing.T) {
	require.NoError(test, newUnitSessionSink(test).Commit(context.Background()))
}

func TestIntegrationOnlyDataTablesAndOneStateRecord(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, false)
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	for _, partID := range []string{"first", "second"} {
		worker := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
		item.PartID = partID
		item.Kind = abstract.InitTableLoad
		require.NoError(test, worker.Push([]abstract.ChangeItem{item, {Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: item.Schema, Table: item.Table, PartID: partID, ColumnValues: []interface{}{uint64(1), uint64(2)}}}))
		item.Kind = abstract.DoneTableLoad
		require.NoError(test, worker.Push([]abstract.ChangeItem{item}))
	}
	keys, err := testStateClient(sink).GetTransferStateKeys("transfer")
	require.NoError(test, err)
	require.Equal(test, []string{sink.publicationStore.TableStateKey(item.TableID())}, keys)
	var temporaryTables []string
	err = sink.tablesClient.NodesByShard()[0][0].DB.QueryRow("SELECT groupArray(name) FROM system.tables WHERE database = ? AND startsWith(name, '__dt_')", sink.dstConfig.Database).Scan(&temporaryTables)
	require.NoError(test, err)
	require.Len(test, temporaryTables, 2, "one data table per part, no journal or ready-marker tables")
	for _, tableName := range temporaryTables {
		count, err := sink.tablesClient.CountPhysicalRows(context.Background(), "127.0.0.1", tableName)
		require.NoError(test, err)
		require.EqualValues(test, 1, count)
		var columnNames []string
		err = sink.tablesClient.NodesByShard()[0][0].DB.QueryRow("SELECT groupArray(name) FROM system.columns WHERE database = ? AND table = ?", sink.dstConfig.Database, tableName).Scan(&columnNames)
		require.NoError(test, err)
		require.ElementsMatch(test, []string{"id", "version"}, columnNames)
	}
	require.NoError(test, sink.Commit(context.Background()))
	count, err := sink.tablesClient.CountPhysicalRows(context.Background(), "127.0.0.1", "t")
	require.NoError(test, err)
	require.EqualValues(test, 2, count)
}

func TestIntegrationCleanupDiscoversCreateWithLostReply(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, false)
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	injection := newLostHTTPReply(
		sink.tablesClient.NodesByShard()[0][0].HTTP,
		func(statement string) bool {
			return strings.HasPrefix(statement, "CREATE TABLE ") && strings.Contains(statement, "_loading")
		},
	)
	configureTestNodes(test, sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = injection

		return nodes
	},
	)
	item.Kind = abstract.InitTableLoad
	item.PartID = "lost-create"
	require.ErrorContains(test, sink.Push([]abstract.ChangeItem{item}), "injected lost HTTP command reply")
	require.True(test, injection.lost)
	var count uint64
	require.NoError(test, sink.tablesClient.NodesByShard()[0][0].DB.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__dt_')", sink.dstConfig.Database).Scan(&count))
	require.EqualValues(test, 1, count)
	recovery := newIntegrationWorker(test, sink, "cleanup-operation")
	require.NoError(test, recovery.Cleanup(context.Background()))
	require.NoError(test, sink.tablesClient.NodesByShard()[0][0].DB.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__dt_')", sink.dstConfig.Database).Scan(&count))
	require.Zero(test, count)
	keys, err := testStateClient(sink).GetTransferStateKeys("transfer")
	require.NoError(test, err)
	require.Empty(test, keys)
}

func TestIntegrationCleanupPreservesReplacedPendingResource(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, false)
	ctx := context.Background()
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	resource := ch_eo_common.NewTemporaryTable(defaultSnapshotTable(test, sink).TemporaryTablePrefix(), "127.0.0.1", ch_eo_common.RoleAssembly)
	expectedDDL, err := ch_eo_common.BuildCreateTableDDL(sink.dstConfig.Database, resource.TableName, resource.ExpectedTableUUID, "CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id", "")
	require.NoError(test, err)
	resource.PendingCommand, err = sink.tablesClient.PrepareSessionCommand(ctx, resource.CommandHostName, expectedDDL)
	require.NoError(test, err)
	record := defaultSnapshotTable(test, sink)
	require.NoError(test, record.RecordPendingTableCommand(ctx, resource, resource.PendingCommand))
	replacementUUID := uuid.NewString()
	replacementDDL, err := ch_eo_common.BuildCreateTableDDL(sink.dstConfig.Database, resource.TableName, replacementUUID, "CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id", "")
	require.NoError(test, err)
	_, err = sink.tablesClient.NodesByShard()[0][0].DB.Exec(replacementDDL)
	require.NoError(test, err)
	recovery := newIntegrationWorker(test, sink, "cleanup-operation")
	require.ErrorContains(test, recovery.Cleanup(ctx), "pending table UUID changed")
	actualUUID, err := sink.tablesClient.TableUUID(ctx, resource.CommandHostName, resource.TableName)
	require.NoError(test, err)
	require.Equal(test, replacementUUID, actualUUID)
	keys, err := testStateClient(sink).GetTransferStateKeys("transfer")
	require.NoError(test, err)
	require.Equal(test, []string{sink.publicationStore.TableStateKey(item.TableID())}, keys)
}

func TestIntegrationInterleavedTablesUseTheirOwnStagingSessions(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, false)
	first := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "first", PartID: "part"}
	second := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "second", PartID: "part"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{first, second}))
	firstWorker := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
	secondWorker := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
	first.Kind = abstract.InitTableLoad
	second.Kind = abstract.InitTableLoad
	require.NoError(test, firstWorker.Push([]abstract.ChangeItem{first}))
	require.NoError(test, secondWorker.Push([]abstract.ChangeItem{second}))
	require.NoError(test, firstWorker.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: "public", Table: first.Table, PartID: first.PartID, ColumnValues: []any{uint64(1), uint64(1)}}}))
	require.NoError(test, secondWorker.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: "public", Table: second.Table, PartID: second.PartID, ColumnValues: []any{uint64(2), uint64(1)}}}))
	first.Kind = abstract.DoneTableLoad
	second.Kind = abstract.DoneTableLoad
	require.NoError(test, firstWorker.Push([]abstract.ChangeItem{first}))
	require.NoError(test, secondWorker.Push([]abstract.ChangeItem{second}))
	for _, item := range []abstract.ChangeItem{first, second} {
		ready, err := readReadyAttempt(sink, context.Background(), item.TablePartID())
		require.NoError(test, err)
		require.NotNil(test, ready)
		partTable, rowCount := ready.Shard(0)
		require.EqualValues(test, 1, rowCount)
		require.True(test, strings.HasPrefix(partTable.TableName, loadSnapshotTable(test, sink, item.TableID()).TemporaryTablePrefix()))
	}
	require.NoError(test, sink.Commit(context.Background()))
	for _, tableName := range []string{first.Table, second.Table} {
		count, err := sink.tablesClient.CountPhysicalRows(context.Background(), "127.0.0.1", tableName)
		require.NoError(test, err)
		require.EqualValues(test, 1, count)
	}
}

func readReadyAttempt(sink *Sink, ctx context.Context, tablePartID abstract.TablePartID) (*ch_eo_common.ReadyPart, error) {
	table, err := sink.publicationStore.LoadTable(ctx, sink.publicationStore.TableStateKey(tablePartID.TableID))
	if err != nil {
		return nil, err
	}
	partPrefix := table.TemporaryTablePrefix() + ch_eo_common.Murmur128Hex(tablePartID.PartID) + "_"
	readyNames, err := sink.tablesClient.ReadStrings(
		ctx,
		sink.tablesClient.NodesByShard()[0][0].Host.Name,
		"SELECT name FROM system.tables WHERE database = ? AND startsWith(name, ?) AND endsWith(name, '_ready')",
		sink.dstConfig.Database,
		partPrefix,
	)
	if err != nil {
		return nil, err
	}
	if len(readyNames) == 0 {
		return nil, nil
	}
	if err := sink.initDatabaseOperations(ctx); err != nil {
		return nil, err
	}
	parts, err := part_uploader.DiscoverReady(ctx, sink.tablesClient, sink.operationsForTable(table), sink.dstConfig.Database, table.Snapshot())
	if err != nil {
		return nil, err
	}
	for _, part := range parts {
		partTable, _ := part.Shard(0)
		if strings.HasPrefix(partTable.TableName, partPrefix) {
			return &part, nil
		}
	}
	return nil, nil
}
func loadingPartTable(test *testing.T, sink *Sink, part abstract.TablePartID, shardIndex int) ch_eo_common.OwnedTable {
	test.Helper()
	table := loadSnapshotTable(test, sink, part.TableID)
	host := sink.tablesClient.NodesByShard()[shardIndex][0].Host.Name
	partPrefix := table.TemporaryTablePrefix() + ch_eo_common.Murmur128Hex(part.PartID) + "_"
	loadingNames, err := sink.tablesClient.ReadStrings(
		context.Background(),
		host,
		"SELECT name FROM system.tables WHERE database = ? AND startsWith(name, ?) AND endsWith(name, '_loading')",
		sink.dstConfig.Database,
		partPrefix,
	)
	require.NoError(test, err)
	require.Len(test, loadingNames, 1, "this fixture requires exactly one loading attempt for the part")
	require.NoError(test, sink.initDatabaseOperations(context.Background()))
	host = sink.databaseOperations.PartTableIdentityHostName(host)
	attemptKey := strings.TrimSuffix(strings.TrimPrefix(loadingNames[0], table.TemporaryTablePrefix()), "_loading")
	return *ch_eo_common.NewOwnedTable(host, loadingNames[0], ch_eo_common.PartTableUUID(table.TemporaryTablePrefix(), attemptKey, host))
}
func rewriteStoredTable(test *testing.T, sink *Sink, key string, change func(map[string]any)) {
	test.Helper()
	values, err := testStateClient(sink).GetTransferStateByKeys(sink.transferID, []string{key})
	require.NoError(test, err)
	var record map[string]any
	require.NoError(test, json.Unmarshal([]byte(values[key].Generic.(string)), &record))
	change(record)
	encoded, err := json.Marshal(record)
	require.NoError(test, err)
	require.NoError(test, testStateClient(sink).SetTransferState(sink.transferID, map[string]*coordinator.TransferStateData{key: {Generic: string(encoded)}}))
}

func TestIntegrationConstructorCreatesNativeWriterAndCleanupConnections(test *testing.T) {
	fixture := newIntegrationSink(test, model.Drop, false)
	fixture.destinationDDLFunc = func(abstract.ChangeItem) (string, error) {
		return "CREATE TABLE t (id UInt64, version UInt64) ENGINE = MergeTree() PARTITION BY id ORDER BY id", nil
	}
	dstConfig := fixture.dstConfig
	dstConfig.Hosts = []string{"127.0.0.1"}
	dstConfig.ShardsList = []clickhouse_model.ClickHouseShard{{Name: "s1", Hosts: dstConfig.Hosts}}
	transfer := &model.Transfer{ID: "constructor-transfer", Dst: dstConfig, Type: abstract.TransferTypeSnapshotOnly, Runtime: &abstract.LocalRuntime{}}
	test.Cleanup(func() {
		StopStagingDDLKeepalives(transfer.ID, "dtjconstructor")
		StopStagingDDLKeepalives(transfer.ID, "")
	})
	makeTableNameAndDDLBuilders := func(_ clickhouse_model.ChSinkParams, _ semver.Version) (func(abstract.ChangeItem) string, func(abstract.ChangeItem) (string, error)) {
		return fixture.destinationTableNameFunc, fixture.destinationDDLFunc
	}
	sink, err := NewSinkFromTransfer(
		context.Background(),
		logger.Log,
		solomon.NewRegistry(nil),
		newSerialState(),
		transfer,
		"dtjconstructor",
		false,
		makeTableNameAndDDLBuilders,
	)
	require.NoError(test, err)
	test.Cleanup(func() { require.NoError(test, sink.Close()) })
	require.NotNil(test, sink.uploader)
	schema := abstract.NewTableSchema(
		[]abstract.ColSchema{
			{ColumnName: "id", DataType: "uint64", Required: true},
			{ColumnName: "version", DataType: "uint64", Required: true},
		},
	)
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Table: "t", PartID: "part", TableSchema: schema}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	item.Kind = abstract.InitTableLoad
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	item.Kind = abstract.InsertKind
	item.ColumnNames = []string{"id", "version"}
	// One native block spans more than the default limit of 100 partitions.
	// The constructor must lift that limit before any rows are sent.
	items := make([]abstract.ChangeItem, 150)
	expectedIDs := make([]uint64, len(items))
	for rowIndex := range items {
		items[rowIndex] = item
		items[rowIndex].ColumnValues = []any{uint64(rowIndex), uint64(1)}
		expectedIDs[rowIndex] = uint64(rowIndex)
	}
	require.NoError(test, sink.Push(items))
	item.Kind = abstract.DoneTableLoad
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	require.NoError(test, sink.Commit(context.Background()))
	var storedIDs []uint64
	require.NoError(test, fixture.tablesClient.NodesByShard()[0][0].DB.QueryRow("SELECT arraySort(groupArray(id)) FROM "+fixture.tablesClient.QualifiedTableName("t")).Scan(&storedIDs))
	require.Equal(test, expectedIDs, storedIDs)

	cleanupSink, err := NewSinkFromTransfer(
		context.Background(),
		logger.Log,
		solomon.NewRegistry(nil),
		newSerialState(),
		transfer,
		"",
		true,
		makeTableNameAndDDLBuilders,
	)
	require.NoError(test, err)
	require.NotNil(test, cleanupSink.uploader)
	require.NoError(test, cleanupSink.Close())
	stagingDDLSessions.mutex.Lock()
	_, snapshotHistoryRetained := stagingDDLSessions.byOperation[stagingDDLSessionOperationKey(transfer.ID, "dtjconstructor")]
	_, cleanupHistoryRetained := stagingDDLSessions.byOperation[stagingDDLSessionOperationKey(transfer.ID, "")]
	stagingDDLSessions.mutex.Unlock()
	require.True(test, snapshotHistoryRetained, "snapshot part sinks share their operation's replica failure history")
	require.False(test, cleanupHistoryRetained, "a cleanup sink has no operation owner to release shared replica history")
}

func TestIntegrationCleanupFencesParallelBuildCommands(test *testing.T) {
	sink := newIntegrationSink(test, model.Drop, false)
	ctx := context.Background()
	require.NoError(test, sink.Push([]abstract.ChangeItem{{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}}))
	sink.stopStagingDDLKeepalives()
	record := defaultSnapshotTable(test, sink)

	var pending []*ch_eo_common.OwnedTable
	for i := 0; i < 2; i++ {
		resource := ch_eo_common.NewTemporaryTable(defaultSnapshotTable(test, sink).TemporaryTablePrefix(), "127.0.0.1", ch_eo_common.RoleAssembly)
		command, err := sink.tablesClient.PrepareSessionCommand(ctx, resource.CommandHostName, "CREATE TABLE "+sink.tablesClient.QualifiedTableName(resource.TableName)+" UUID "+ch_eo_common.QuoteStringLiteral(resource.ExpectedTableUUID)+" (id UInt64) ENGINE=MergeTree ORDER BY id")
		require.NoError(test, err)
		resource.PendingCommand = command
		require.NoError(test, record.RecordPendingTableCommand(ctx, resource, resource.PendingCommand))
		pending = append(pending, resource)
	}
	recovery := newIntegrationWorker(test, sink, "cleanup-operation")
	require.NoError(test, recovery.Cleanup(ctx))
	for _, resource := range pending {
		err := recovery.tablesClient.ExecuteCommandOverHTTP(ctx, resource.CommandHostName, resource.PendingCommand.SQL, ch_eo_common.ExistingSessionParams(resource.PendingCommand.SessionID))
		var exception *clickhouse_go.Exception
		require.ErrorAs(test, err, &exception)
		require.EqualValues(test, sessionNotFoundCode, exception.Code)
		actualUUID, err := recovery.tablesClient.TableUUID(ctx, resource.CommandHostName, resource.TableName)
		require.NoError(test, err)
		require.Empty(test, actualUUID)
	}
}

// Test fixtures rebuild clients to inject topology/transports without exposing
// mutable production topology. Rebind the strategy to the new client as well.
func configureTestNodes(test *testing.T, sink *Sink, configure func([][]ch_eo_common.Node) [][]ch_eo_common.Node) {
	test.Helper()
	nodes := configure(sink.tablesClient.NodesByShard())
	sink.tablesClient = ch_eo_common.NewClient(sink.dstConfig, sink.logger, nodes)
	rebuildTestUploader(test, sink)
	sink.databaseOperations = nil
}

func TestIntegrationCommitMultipleTables(test *testing.T) {
	for _, sameDestination := range []bool{false, true} {
		test.Run(fmt.Sprintf("same_destination=%t", sameDestination), func(test *testing.T) {
			sink := newIntegrationSink(test, model.Drop, false)
			ctx := context.Background()
			for tableIndex, tableName := range []string{"t", "second_table"} {
				item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: tableName}
				require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
				item.Kind = abstract.InitTableLoad
				rebuildTestUploader(test, sink)
				item.PartID = "part"
				require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
				row := item
				row.Kind = abstract.InsertKind
				row.TableSchema = snapshotTestSchema
				row.ColumnNames = []string{"id", "version"}
				row.ColumnValues = []any{uint64(tableIndex + 1), uint64(1)}
				require.NoError(test, sink.Push([]abstract.ChangeItem{row}))
				item.Kind = abstract.DoneTableLoad
				require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			}
			if sameDestination {
				// Initialization already rejects collisions. Inject conflicting state
				// to verify Commit also rejects it before publishing either table.
				secondTable := loadSnapshotTable(test, sink, abstract.TableID{Namespace: "public", Name: "second_table"})
				rewriteStoredTable(test, sink, secondTable.StateKey(), func(record map[string]any) { record["DestinationTableName"] = "t" })
				require.ErrorContains(test, sink.Commit(context.Background()), "map to the same destination")
				currentUUID, err := sink.tablesClient.TableUUID(ctx, "127.0.0.1", "t")
				require.NoError(test, err)
				require.Empty(test, currentUUID)
				require.NoError(test, sink.Cleanup(ctx))
			} else {
				require.NoError(test, sink.Commit(context.Background()))
				for tableIndex, tableName := range []string{"t", "second_table"} {
					var actualID uint64
					err := sink.tablesClient.NodesByShard()[0][0].DB.QueryRow("SELECT id FROM " + sink.tablesClient.QualifiedTableName(tableName)).Scan(&actualID)
					require.NoError(test, err)
					require.EqualValues(test, tableIndex+1, actualID)
				}
			}
			stateKeys, err := sink.publicationStore.ListStateKeysWithPrefix(ctx, transfer_state.StatePrefix)
			require.NoError(test, err)
			require.Empty(test, stateKeys)
		})
	}
}

func newTwoShardAtomicIntegrationSink(test *testing.T) *Sink {
	test.Helper()
	if os.Getenv("DT_SNAPSHOT_CLICKHOUSE_REPLICAS_TEST") != "1" {
		test.Skip("requires two ClickHouse servers")
	}
	sink := newIntegrationSink(test, model.Drop, false)
	destination := clickhouse_model.ChDestination{Database: "default", User: "default", NativePort: 19001, HTTPPort: 18124}
	params, err := destination.ToSinkParams(&model.Transfer{})
	require.NoError(test, err)
	host := &conn_clickhouse.Host{Name: "localhost", NativePort: 19001, HTTPPort: 18124}
	database, err := conn.ConnectNative(host, params)
	require.NoError(test, err)
	_, err = database.Exec("CREATE DATABASE " + ch_eo_common.QuoteIdentifier(sink.dstConfig.Database) + " ENGINE = Atomic")
	require.NoError(test, err)
	test.Cleanup(
		func() {
			_, dropErr := database.Exec("DROP DATABASE " + ch_eo_common.QuoteIdentifier(sink.dstConfig.Database) + " SYNC")
			require.NoError(test, dropErr)
			require.NoError(test, database.Close())
		},
	)
	destination.Database = sink.dstConfig.Database
	params, err = destination.ToSinkParams(&model.Transfer{})
	require.NoError(test, err)
	client, err := httpclient.NewHTTPClientImpl(params)
	require.NoError(test, err)
	configureTestNodes(test, sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes = append(nodes, []ch_eo_common.Node{ch_eo_common.NewNode(host, database, client)})

		return nodes
	},
	)
	configureTestSharder(test, sink, func(row abstract.ChangeItem) sharding.ShardID {
		return sharding.ShardID(row.ColumnValues[0].(uint64) % 2)
	})
	return sink
}

func TestIntegrationPartRenameLostReplyRecoveredAcrossShards(test *testing.T) {
	sink := newTwoShardAtomicIntegrationSink(test)
	sink.destinationDDLFunc = func(abstract.ChangeItem) (string, error) {
		return "CREATE TABLE t (id UInt64 COMMENT 'source column documentation', version UInt64 COMMENT 'source version') ENGINE = MergeTree ORDER BY id", nil
	}
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t", PartID: "source/part?with:special\\characters"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	worker := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
	item.Kind = abstract.InitTableLoad
	rows := []abstract.ChangeItem{
		{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: item.Schema, Table: item.Table, PartID: item.PartID, ColumnValues: []any{uint64(0), uint64(1)}},
		{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: item.Schema, Table: item.Table, PartID: item.PartID, ColumnValues: []any{uint64(1), uint64(1)}},
	}
	injection := newLostHTTPReply(
		worker.tablesClient.NodesByShard()[0][0].HTTP,
		func(statement string) bool {
			return strings.HasPrefix(statement, "RENAME TABLE") && strings.Contains(statement, "_loading") && strings.Contains(statement, "_ready")
		},
	)
	configureTestNodes(test, worker, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = injection
		return nodes
	},
	)
	require.NoError(test, worker.Push([]abstract.ChangeItem{item}))
	require.NoError(test, worker.Push(rows))
	item.Kind = abstract.DoneTableLoad
	require.ErrorContains(test, worker.Push([]abstract.ChangeItem{item}), "injected lost HTTP command reply")
	require.True(test, injection.lost)
	worker.stopStagingDDLKeepalives()

	// Renames run in parallel: the other shard may already be ready as well.
	// Every shard was verified before renaming, so reassignment must not rewrite.
	for shardIndex, nodes := range sink.tablesClient.NodesByShard() {
		var readyTables uint64
		require.NoError(test, nodes[0].DB.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__dt_') AND endsWith(name, '_ready')", sink.dstConfig.Database).Scan(&readyTables))
		if shardIndex == 0 {
			require.EqualValues(test, 1, readyTables)
		} else {
			require.LessOrEqual(test, readyTables, uint64(1))
		}
	}
	retryWorker := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
	setTestAppendRows(test, retryWorker, func(ch_eo_common.Node, string, []abstract.ChangeItem) error {
		return xerrors.New("verified part must not be rewritten after a lost rename reply")
	},
	)
	item.Kind = abstract.InitTableLoad
	require.NoError(test, retryWorker.Push([]abstract.ChangeItem{item}))
	require.NoError(test, retryWorker.Push(rows))
	item.Kind = abstract.DoneTableLoad
	require.NoError(test, retryWorker.Push([]abstract.ChangeItem{item}))
	for _, nodes := range sink.tablesClient.NodesByShard() {
		var names []string
		require.NoError(test, nodes[0].DB.QueryRow("SELECT groupArray(name) FROM system.tables WHERE database = ? AND startsWith(name, '__dt_')", sink.dstConfig.Database).Scan(&names))
		require.Len(test, names, 1)
		require.Contains(test, names[0], "_ready")
		var comments []string
		require.NoError(test, nodes[0].DB.QueryRow("SELECT groupArray(comment) FROM system.columns WHERE database = ? AND table = ?", sink.dstConfig.Database, names[0]).Scan(&comments))
		require.ElementsMatch(test, []string{"source column documentation", "source version"}, comments, "data column comments must not contain snapshot metadata")
	}
	require.NoError(test, sink.Commit(context.Background()))
	for shardIndex, nodes := range sink.tablesClient.NodesByShard() {
		var actualIDs []uint64
		require.NoError(test, nodes[0].DB.QueryRow("SELECT arraySort(groupArray(id)) FROM "+sink.tablesClient.QualifiedTableName("t")).Scan(&actualIDs))
		require.Equal(test, []uint64{uint64(shardIndex)}, actualIDs)
		var remaining uint64
		require.NoError(test, nodes[0].DB.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__dt_')", sink.dstConfig.Database).Scan(&remaining))
		require.Zero(test, remaining)
	}
	operations, err := sink.publicationStore.ListTableStateKeysByOperation(context.Background())
	require.NoError(test, err)
	require.Empty(test, operations)
}

func TestIntegrationPartMissingOnFirstShardFailsCommit(test *testing.T) {
	sink := newTwoShardAtomicIntegrationSink(test)
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Table: "t", PartID: "part"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	worker := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
	item.Kind = abstract.InitTableLoad
	require.NoError(test, worker.Push([]abstract.ChangeItem{item}))
	require.NoError(test, worker.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Table: "t", PartID: "part", ColumnValues: []any{uint64(1), uint64(1)}}}))
	item.Kind = abstract.DoneTableLoad
	require.NoError(test, worker.Push([]abstract.ChangeItem{item}))
	ready, err := readReadyAttempt(sink, context.Background(), item.TablePartID())
	require.NoError(test, err)
	resource, _ := ready.Shard(0)
	_, err = sink.tablesClient.NodesByShard()[0][0].DB.Exec("DROP TABLE " + sink.tablesClient.QualifiedTableName(resource.TableName) + " SYNC")
	require.NoError(test, err)
	require.ErrorContains(test, sink.Commit(context.Background()), "verified part is missing on a shard")
}

// lostHTTPReply executes the selected SQL command but loses its first successful response.
type lostHTTPReply struct {
	httpclient.HTTPClient
	matchStatement func(string) bool
	lost           bool
	cancel         context.CancelFunc
}

func newLostHTTPReply(client httpclient.HTTPClient, matchStatement func(string) bool) *lostHTTPReply {
	return &lostHTTPReply{
		HTTPClient:     client,
		matchStatement: matchStatement,
		lost:           false,
		cancel:         nil,
	}
}

func (c *lostHTTPReply) QueryStream(ctx context.Context, logger log.Logger, host *conn_clickhouse.Host, query any, params url.Values) (io.ReadCloser, error) {
	body, err := c.HTTPClient.QueryStream(ctx, logger, host, query, params)
	if err != nil {
		if body != nil {
			_ = body.Close()
		}
		return nil, err
	}
	if c.lost || !c.matchStatement(fmt.Sprint(query)) {
		return body, nil
	}
	_, readErr := io.Copy(io.Discard, body)
	closeErr := body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	c.lost = true
	if c.cancel != nil {
		c.cancel()
	}
	return nil, xerrors.New("injected lost HTTP command reply")
}

// lostPublicationStateReply stores the publication decision, then loses its ACK.
type lostPublicationStateReply struct {
	coordinator.TransferState
	lost bool
}

func newLostPublicationStateReply(client coordinator.TransferState) *lostPublicationStateReply {
	return &lostPublicationStateReply{TransferState: client}
}

func (s *lostPublicationStateReply) SetTransferState(transferID string, values map[string]*coordinator.TransferStateData) error {
	if err := s.TransferState.SetTransferState(transferID, values); err != nil {
		return err
	}
	for _, value := range values {
		record, ok := value.Generic.(string)
		if !ok {
			continue
		}
		var tableState struct{ PublicationStatus string }
		if err := json.Unmarshal([]byte(record), &tableState); err != nil {
			return err
		}
		if !s.lost && tableState.PublicationStatus == "decided" {
			s.lost = true
			return xerrors.New("injected lost publication state reply")
		}
	}
	return nil
}

func TestIntegrationReplicatedReplicaLostInsertReply(test *testing.T) {
	if os.Getenv("DT_SNAPSHOT_REPLICATED_DATABASE_TEST") != "1" {
		test.Skip("requires four ClickHouse servers with a shared Keeper")
	}
	test.Setenv("DT_SNAPSHOT_CLICKHOUSE_TEST", "1")
	sink := newReplicatedMultiReplicaSink(test)
	ctx := context.Background()
	primary := sink.tablesClient.NodesByShard()[0][0]
	_, err := primary.DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE=ReplicatedMergeTree ORDER BY id")
	require.NoError(test, err)
	require.NoError(test, sink.databaseOperations.WaitForDatabaseDDL(ctx))
	for shardIndex, nodes := range sink.tablesClient.NodesByShard() {
		_, err := nodes[0].DB.Exec("INSERT INTO "+sink.tablesClient.QualifiedTableName("t")+" VALUES (?, 1)", uint64(100+shardIndex))
		require.NoError(test, err)
	}
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Table: "t", PartID: "ambiguous"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	item.Kind = abstract.InitTableLoad
	writeRows := testWriteRows(sink)
	failedHost := sink.tablesClient.NodesByShard()[1][0].Host.Name
	writeError := xerrors.New("injected lost replica INSERT reply")
	setTestAppendRows(test, sink, func(node ch_eo_common.Node, tableName string, rows []abstract.ChangeItem) error {
		if err := writeRows(node, tableName, rows); err != nil {
			return xerrors.Errorf("write before losing replica reply: %w", err)
		}
		if node.Host.Name == failedHost {
			return writeError
		}
		return nil
	},
	)
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	rows := []abstract.ChangeItem{
		{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Table: "t", PartID: item.PartID, ColumnValues: []any{uint64(0), uint64(1)}},
		{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Table: "t", PartID: item.PartID, ColumnValues: []any{uint64(1), uint64(1)}},
	}
	require.ErrorIs(test, sink.Push(rows), writeError)
	require.ErrorIs(test, sink.Push(rows), writeError)
	item.Kind = abstract.DoneTableLoad
	require.ErrorIs(test, sink.Push([]abstract.ChangeItem{item}), writeError)
	require.Error(test, sink.Commit(context.Background()))
	ready, err := readReadyAttempt(sink, ctx, item.TablePartID())
	require.NoError(test, err)
	require.Nil(test, ready)
	recovery := newIntegrationWorker(test, sink, "cleanup-operation")
	require.NoError(test, recovery.Cleanup(ctx))
	for shardIndex, nodes := range sink.tablesClient.NodesByShard() {
		for _, node := range nodes {
			var actualIDs []uint64
			require.NoError(test, node.DB.QueryRow("SELECT arraySort(groupArray(id)) FROM "+sink.tablesClient.QualifiedTableName("t")).Scan(&actualIDs))
			require.Equal(test, []uint64{uint64(100 + shardIndex)}, actualIDs)
			var temporaryTables uint64
			require.NoError(test, node.DB.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__dt_')", sink.dstConfig.Database).Scan(&temporaryTables))
			require.Zero(test, temporaryTables)
		}
	}
}

func TestIntegrationReplicatedDatabase(test *testing.T) {
	if os.Getenv("DT_SNAPSHOT_REPLICATED_DATABASE_TEST") != "1" {
		test.Skip("requires two ClickHouse 26.3 servers with a shared Keeper")
	}
	test.Setenv("DT_SNAPSHOT_CLICKHOUSE_TEST", "1")
	for _, cleanup := range []model.CleanupType{model.Drop, model.Truncate, model.DisabledCleanup} {
		for _, existing := range []bool{false, true} {
			test.Run(fmt.Sprintf("%s/existing=%t", cleanup, existing), func(test *testing.T) {
				test.Parallel()
				runReplicatedDatabaseSnapshot(test, cleanup, existing, false)
			})
		}
	}
	test.Run("two_replicas", func(test *testing.T) {
		test.Parallel()
		runReplicatedDatabaseSnapshot(test, model.DisabledCleanup, true, true)
	})
}

func runReplicatedDatabaseSnapshot(test *testing.T, cleanup model.CleanupType, existing bool, sameShard bool) {
	sink := newReplicatedDatabaseSink(test, cleanup, sameShard)
	database := sink.dstConfig.Database
	primary := sink.tablesClient.NodesByShard()[0][0]
	ctx := context.Background()
	var err error
	if existing {
		_, err = primary.DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64,version UInt64) ENGINE=ReplicatedReplacingMergeTree(version) ORDER BY id")
		require.NoError(test, err)
		require.NoError(test, sink.databaseOperations.WaitForDatabaseDDL(ctx))
		// Completing database DDL does not wait for the table's Keeper session.
		// Start the test only once the existing target is writable on each replica.
		for _, nodes := range sink.tablesClient.NodesByShard() {
			for _, node := range nodes {
				require.Eventually(test, func() bool {
					var writableReplicas uint64
					queryError := node.DB.QueryRow("SELECT count() FROM system.replicas WHERE database = ? AND table = 't' AND NOT is_readonly AND NOT is_session_expired", database).Scan(&writableReplicas)
					return queryError == nil && writableReplicas == 1
				}, 30*time.Second, 100*time.Millisecond)
			}
			_, err = nodes[0].DB.Exec("INSERT INTO " + sink.tablesClient.QualifiedTableName("t") + " VALUES (10,1)")
			require.NoError(test, err)
		}
	}
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Table: "t"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	for partIndex := 0; partIndex < 2; partIndex++ {
		item.PartID = fmt.Sprint(partIndex)
		item.Kind = abstract.InitTableLoad
		rebuildTestUploader(test, sink)
		require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
		stage := loadingPartTable(test, sink, item.TablePartID(), 0)
		for _, nodes := range sink.tablesClient.NodesByShard() {
			for _, node := range nodes {
				require.NoError(test, sink.tablesClient.CheckTableUUIDAndRowCount(ctx, node.Host.Name, stage.TableName, stage.ExpectedTableUUID, 0))
				var stageTables uint64
				require.NoError(test, node.DB.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__dt_') AND (endsWith(name, '_loading') OR endsWith(name, '_ready'))", database).Scan(&stageTables))
				require.Equal(test, uint64(partIndex+1), stageTables)
			}
		}
		rows := make([]abstract.ChangeItem, 0)
		for rowIndex := 0; rowIndex < 4; rowIndex++ {
			rows = append(rows, abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Table: "t", PartID: item.PartID, ColumnValues: []any{uint64(partIndex*4 + rowIndex), uint64(1)}})
		}
		require.NoError(test, sink.Push(rows))
		item.Kind = abstract.DoneTableLoad
		require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
		ready, err := readReadyAttempt(sink, ctx, item.TablePartID())
		require.NoError(test, err)
		require.NotNil(test, ready)
		for shardIndex := 0; shardIndex < ready.ShardCount(); shardIndex++ {
			readyTable, _ := ready.Shard(shardIndex)
			require.Equal(test, stage.ExpectedTableUUID, readyTable.ExpectedTableUUID)
			require.Equal(test, strings.TrimSuffix(stage.TableName, "_loading")+"_ready", readyTable.TableName)
		}
	}
	require.NoError(test, sink.Commit(context.Background()))
	for shardIndex, nodes := range sink.tablesClient.NodesByShard() {
		for _, node := range nodes {
			expected := uint64(8 / len(sink.tablesClient.NodesByShard()))
			if existing && cleanup == model.DisabledCleanup {
				expected++
			}
			count, err := sink.tablesClient.CountPhysicalRows(ctx, node.Host.Name, "t")
			require.NoError(test, err)
			require.Equal(test, expected, count)
			expectedIDs := make([]uint64, 0)
			for rowID := uint64(0); rowID < 8; rowID++ {
				if rowID%uint64(len(sink.tablesClient.NodesByShard())) == uint64(shardIndex) {
					expectedIDs = append(expectedIDs, rowID)
				}
			}
			if existing && cleanup == model.DisabledCleanup {
				expectedIDs = append(expectedIDs, 10)
			}
			var actualIDs []uint64
			require.NoError(test, node.DB.QueryRow("SELECT arraySort(groupArray(id)) FROM "+sink.tablesClient.QualifiedTableName("t")).Scan(&actualIDs))
			require.Equal(test, expectedIDs, actualIDs)

			var temporaryTables uint64
			require.NoError(test, node.DB.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__dt_')", database).Scan(&temporaryTables))
			require.Zero(test, temporaryTables)
		}
	}
	if existing {
		require.True(test, primary.HTTP.(*lostHTTPReply).lost)
	}
	operations, err := sink.publicationStore.ListTableStateKeysByOperation(context.Background())
	require.NoError(test, err)
	require.Empty(test, operations)
}

func newReplicatedDatabaseSink(test *testing.T, cleanup model.CleanupType, sameShard bool) *Sink {
	sink := newIntegrationSink(test, cleanup, true)
	// The fixture replaces its Atomic database with a Replicated database.
	sink.databaseOperations = nil
	database := sink.dstConfig.Database
	primary := sink.tablesClient.NodesByShard()[0][0]
	_, err := primary.DB.Exec("DROP DATABASE " + ch_eo_common.QuoteIdentifier(database) + " SYNC")
	require.NoError(test, err)
	secondaryShard := "shard1"
	if sameShard {
		secondaryShard = "shard0"
	}
	keeperPath := "/snapshot_replicated_tests/" + database
	_, err = primary.DB.Exec("CREATE DATABASE " + ch_eo_common.QuoteIdentifier(database) + " ENGINE=Replicated(" + ch_eo_common.QuoteStringLiteral(keeperPath) + ", 'shard0', 'replica0')")
	require.NoError(test, err)
	dst := clickhouse_model.ChDestination{Database: "default", User: "default", NativePort: 19001, HTTPPort: 18124}
	params, err := dst.ToSinkParams(&model.Transfer{})
	require.NoError(test, err)
	host := &conn_clickhouse.Host{Name: "localhost", NativePort: 19001, HTTPPort: 18124}
	db, err := conn.ConnectNative(host, params)
	require.NoError(test, err)
	test.Cleanup(
		func() {
			_, dropErr := db.Exec("DROP DATABASE IF EXISTS " + ch_eo_common.QuoteIdentifier(database) + " SYNC")
			require.NoError(test, dropErr)
			require.NoError(test, db.Close())
		},
	)
	_, err = db.Exec("CREATE DATABASE " + ch_eo_common.QuoteIdentifier(database) + " ENGINE=Replicated(" + ch_eo_common.QuoteStringLiteral(keeperPath) + ", " + ch_eo_common.QuoteStringLiteral(secondaryShard) + ", 'replica1')")
	require.NoError(test, err)
	dst.Database = database
	params, err = dst.ToSinkParams(&model.Transfer{})
	require.NoError(test, err)
	client, err := httpclient.NewHTTPClientImpl(params)
	require.NoError(test, err)
	if sameShard {
		configureTestNodes(test, sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
			nodes[0] = append(nodes[0], ch_eo_common.NewNode(host, db, client))

			return nodes
		},
		)
	} else {
		configureTestNodes(test, sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
			nodes = append(nodes, []ch_eo_common.Node{ch_eo_common.NewNode(host, db, client)})

			return nodes
		},
		)
	}
	sink.defaultReplicatedTableEngine = true
	configureTestSharder(test, sink, func(item abstract.ChangeItem) sharding.ShardID {
		return sharding.ShardID(item.ColumnValues[0].(uint64) % uint64(len(sink.tablesClient.NodesByShard())))
	})
	require.Eventually(
		test,
		func() bool {
			var count uint64
			return primary.DB.QueryRow("SELECT total_replicas FROM system.database_replicas WHERE database = ?", database).Scan(&count) == nil && count == 2
		}, 10*time.Second,
		100*time.Millisecond,
	)
	ctx := context.Background()
	require.NoError(test, sink.initDatabaseOperations(ctx))
	return sink
}

func TestIntegrationReplicatedDatabaseCleanup(test *testing.T) {
	if os.Getenv("DT_SNAPSHOT_REPLICATED_DATABASE_TEST") != "1" {
		test.Skip("requires two ClickHouse servers with a shared Keeper")
	}
	test.Setenv("DT_SNAPSHOT_CLICKHOUSE_TEST", "1")
	sink := newReplicatedDatabaseSink(test, model.Drop, false)
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Table: "t"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	item.Kind = abstract.InitTableLoad
	item.PartID = "unfinished"
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	require.NoError(test, sink.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Table: "t", PartID: item.PartID, ColumnValues: []any{uint64(1), uint64(1)}}}))
	recovery := newIntegrationWorker(test, sink, "cleanup-operation")
	require.NoError(test, recovery.Cleanup(context.Background()))
	for _, nodes := range sink.tablesClient.NodesByShard() {
		for _, node := range nodes {
			var tableCount uint64
			require.NoError(test, node.DB.QueryRow("SELECT count() FROM system.tables WHERE database = ?", sink.dstConfig.Database).Scan(&tableCount))
			require.Zero(test, tableCount)
		}
	}
	operations, err := sink.publicationStore.ListTableStateKeysByOperation(context.Background())
	require.NoError(test, err)
	require.Empty(test, operations)
}

func TestIntegrationReplicatedDatabaseIdenticalParts(test *testing.T) {
	if os.Getenv("DT_SNAPSHOT_REPLICATED_DATABASE_TEST") != "1" {
		test.Skip("requires two ClickHouse servers with a shared Keeper")
	}
	test.Setenv("DT_SNAPSHOT_CLICKHOUSE_TEST", "1")
	sink := newReplicatedDatabaseSink(test, model.Drop, true)
	sink.destinationDDLFunc = func(abstract.ChangeItem) (string, error) {
		return "CREATE TABLE t (id UInt64, version UInt64) ENGINE=MergeTree ORDER BY id", nil
	}
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Table: "t"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	for partIndex := 0; partIndex < 2; partIndex++ {
		item.Kind, item.PartID = abstract.InitTableLoad, fmt.Sprint(partIndex)
		rebuildTestUploader(test, sink)
		require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
		require.NoError(test, sink.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Table: "t", PartID: item.PartID, ColumnValues: []any{uint64(1), uint64(1)}}}))
		item.Kind = abstract.DoneTableLoad
		require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	}
	require.NoError(test, sink.Commit(context.Background()))
	for _, node := range sink.tablesClient.NodesByShard()[0] {
		rows, err := sink.tablesClient.CountPhysicalRows(context.Background(), node.Host.Name, "t")
		require.NoError(test, err)
		require.EqualValues(test, 2, rows)
	}
}

func newReplicatedMultiReplicaSink(test *testing.T) *Sink {
	test.Helper()
	sink := newReplicatedDatabaseSink(test, model.DisabledCleanup, false)
	database := sink.dstConfig.Database
	keeperPath := "/snapshot_replicated_tests/" + database
	// Node identity currently uses host names. These distinct loopback aliases
	// connect to the two additional servers without changing production routing.
	hostNames := []string{"localhost.", "LOCALHOST"}
	for shardIndex, hostName := range hostNames {
		nativePort, httpPort := 19002+shardIndex, 18125+shardIndex
		dst := clickhouse_model.ChDestination{Database: "default", User: "default", NativePort: nativePort, HTTPPort: httpPort}
		params, err := dst.ToSinkParams(&model.Transfer{})
		require.NoError(test, err)
		host := &conn_clickhouse.Host{Name: hostName, NativePort: nativePort, HTTPPort: httpPort}
		db, err := conn.ConnectNative(host, params)
		require.NoError(test, err)
		test.Cleanup(
			func() {
				_, dropErr := db.Exec("DROP DATABASE IF EXISTS " + ch_eo_common.QuoteIdentifier(database) + " SYNC")
				require.NoError(test, dropErr)
				require.NoError(test, db.Close())
			},
		)
		statement := fmt.Sprintf("CREATE DATABASE %s ENGINE=Replicated(%s, 'shard%d', 'replica%d')", ch_eo_common.QuoteIdentifier(database), ch_eo_common.QuoteStringLiteral(keeperPath), shardIndex, shardIndex+2)
		_, err = db.Exec(statement)
		require.NoError(test, err)
		dst.Database = database
		params, err = dst.ToSinkParams(&model.Transfer{})
		require.NoError(test, err)
		client, err := httpclient.NewHTTPClientImpl(params)
		require.NoError(test, err)
		configureTestNodes(test, sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
			nodes[shardIndex] = append(nodes[shardIndex], ch_eo_common.NewNode(host, db, client))

			return nodes
		},
		)
	}
	for _, nodes := range sink.tablesClient.NodesByShard() {
		for _, node := range nodes {
			require.Eventually(
				test,
				func() bool {
					var replicaCount uint64
					return node.DB.QueryRow("SELECT total_replicas FROM system.database_replicas WHERE database = ?", database).Scan(&replicaCount) == nil && replicaCount == 4
				}, 10*time.Second,
				100*time.Millisecond,
			)
		}
	}
	sink.databaseOperations = nil
	require.NoError(test, sink.initDatabaseOperations(context.Background()))
	return sink
}

// Replicated database DDL elects any replica of each target shard for ATTACH
// into ReplicatedMergeTree. Every eligible replica must have the source rows.
func TestIntegrationReplicatedDatabaseMultipleShardsAndReplicas(test *testing.T) {
	if os.Getenv("DT_SNAPSHOT_REPLICATED_DATABASE_TEST") != "1" {
		test.Skip("requires four ClickHouse servers with a shared Keeper")
	}
	test.Setenv("DT_SNAPSHOT_CLICKHOUSE_TEST", "1")
	sink := newReplicatedMultiReplicaSink(test)
	// This scenario inspects replicated loading rows before the completion event.
	setTestAppendRows(test, sink, testWriteRows(sink))
	ctx := context.Background()
	primary := sink.tablesClient.NodesByShard()[0][0]
	_, err := primary.DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE=ReplicatedMergeTree ORDER BY id PARTITION BY intDiv(id, 2)")
	require.NoError(test, err)
	require.NoError(test, sink.databaseOperations.WaitForDatabaseDDL(ctx))
	for shardIndex, nodes := range sink.tablesClient.NodesByShard() {
		_, err := nodes[0].DB.Exec("INSERT INTO "+sink.tablesClient.QualifiedTableName("t")+" VALUES (?, 1)", uint64(100+shardIndex))
		require.NoError(test, err)
	}
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Table: "t"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	// Each shard has distinct snapshot partitions, including partitions absent
	// on the replica that initiates the database-wide ATTACH.
	for partIndex := 0; partIndex < 2; partIndex++ {
		item.Kind, item.PartID = abstract.InitTableLoad, fmt.Sprint(partIndex)
		rebuildTestUploader(test, sink)
		require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
		var rows []abstract.ChangeItem
		for rowIndex := 0; rowIndex < 4; rowIndex++ {
			rows = append(rows, abstract.ChangeItem{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Table: "t", PartID: item.PartID, ColumnValues: []any{uint64(partIndex*8 + rowIndex*2 + rowIndex%2), uint64(1)}})
		}
		require.NoError(test, sink.Push(rows))
		for shardIndex, nodes := range sink.tablesClient.NodesByShard() {
			stage := loadingPartTable(test, sink, item.TablePartID(), shardIndex)
			for _, node := range nodes {
				require.NoError(test, sink.tablesClient.ExecuteCommandOverHTTP(ctx, node.Host.Name, "SYSTEM SYNC REPLICA "+sink.tablesClient.QualifiedTableName(stage.TableName), nil))
				require.NoError(test, sink.tablesClient.CheckTableUUIDAndRowCount(ctx, node.Host.Name, stage.TableName, stage.ExpectedTableUUID, 2))
			}
		}
		item.Kind = abstract.DoneTableLoad
		require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	}
	require.NoError(test, sink.Commit(context.Background()))
	for shardIndex, nodes := range sink.tablesClient.NodesByShard() {
		expectedIDs := []uint64{uint64(shardIndex * 3), uint64(shardIndex*3 + 4), uint64(shardIndex*3 + 8), uint64(shardIndex*3 + 12), uint64(shardIndex + 100)}
		for _, node := range nodes {
			var actualIDs []uint64
			require.NoError(test, node.DB.QueryRow("SELECT arraySort(groupArray(id)) FROM "+sink.tablesClient.QualifiedTableName("t")).Scan(&actualIDs))
			require.Equal(test, expectedIDs, actualIDs)
			var temporaryTables uint64
			require.NoError(test, node.DB.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__dt_')", sink.dstConfig.Database).Scan(&temporaryTables))
			require.Zero(test, temporaryTables)
		}
	}
	operations, err := sink.publicationStore.ListTableStateKeysByOperation(context.Background())
	require.NoError(test, err)
	require.Empty(test, operations)
}

const (
	unknownSettingCode  = 115
	sessionNotFoundCode = 372
	sessionLockedCode   = 373
)

type sessionHTTPClient struct {
	httpclient.HTTPClient
	query func(url.Values) error
}

func newSessionHTTPClient(query func(url.Values) error) *sessionHTTPClient {
	return &sessionHTTPClient{HTTPClient: nil, query: query}
}

func (c *sessionHTTPClient) QueryStream(_ context.Context, _ log.Logger, _ *conn_clickhouse.Host, query any, params url.Values) (io.ReadCloser, error) {
	if queryText, ok := query.(string); ok && queryText == "SELECT 1 FORMAT Null" && params.Get("session_id") == "" && params.Get("query_id") == "" {
		return io.NopCloser(strings.NewReader("")), nil
	}
	err := c.query(params)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(strings.NewReader("")), nil
}

type lostShardAttachReply struct {
	httpclient.HTTPClient
	failed bool
}

func newLostShardAttachReply(client httpclient.HTTPClient) *lostShardAttachReply {
	return &lostShardAttachReply{HTTPClient: client, failed: false}
}

func (c *lostShardAttachReply) QueryStream(ctx context.Context, logger log.Logger, host *conn_clickhouse.Host, query any, params url.Values) (io.ReadCloser, error) {
	response, err := c.HTTPClient.QueryStream(ctx, logger, host, query, params)
	if err != nil {
		return response, err
	}
	statement, _ := query.(string)
	if strings.HasPrefix(statement, "ALTER TABLE ") && strings.Contains(statement, "ATTACH PARTITION") && strings.Contains(statement, string(ch_eo_common.RoleAssembly)) && !c.failed {
		// Confirm actual server execution, then hide the successful response.
		_, err = io.ReadAll(response)
		_ = response.Close()
		if err != nil {
			return nil, err
		}
		c.failed = true
		return nil, io.ErrUnexpectedEOF
	}
	return response, nil
}

func TestIntegrationShardRetryDoesNotDuplicateAttachedRows(test *testing.T) {
	sink := newIntegrationSink(test, model.DisabledCleanup, false)
	ctx := context.Background()
	node := &sink.tablesClient.NodesByShard()[0][0]
	_, err := node.DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE=MergeTree ORDER BY id")
	require.NoError(test, err)
	_, err = node.DB.Exec("INSERT INTO " + sink.tablesClient.QualifiedTableName("t") + " VALUES (9,1)")
	require.NoError(test, err)
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	item.Kind = abstract.InitTableLoad
	item.PartID = "part"
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	require.NoError(test, sink.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Schema: "public", Table: "t", PartID: "part", ColumnValues: []any{uint64(1), uint64(1)}}}))
	item.Kind = abstract.DoneTableLoad
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	record := loadSnapshotTable(test, sink, item.TableID())
	lostReply := newLostShardAttachReply(node.HTTP)
	configureTestNodes(test, sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = lostReply
		return nodes
	},
	)
	require.NoError(test, buildTestCandidates(test, sink, record))
	require.True(test, lostReply.failed)
	require.NoError(test, sink.tablesClient.CheckTableUUIDAndRowCount(ctx, node.Host.Name, record.Replicas()[0].CandidateTable.TableName, record.Replicas()[0].CandidateTable.ExpectedTableUUID, 2))
	// Target remains untouched until the publication decision.
	rows, err := sink.tablesClient.CountPhysicalRows(ctx, node.Host.Name, "t")
	require.NoError(test, err)
	require.EqualValues(test, 1, rows)
	require.NoError(test, sink.Cleanup(ctx))
}

func TestIntegrationStreamingWritesFinishBeforeReady(test *testing.T) {
	for _, lostReply := range []bool{false, true} {
		test.Run(fmt.Sprintf("lost_reply=%t", lostReply), func(test *testing.T) {
			sink := newIntegrationSink(test, model.DisabledCleanup, false)
			ctx := context.Background()
			node := sink.tablesClient.NodesByShard()[0][0]
			_, err := node.DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE=MergeTree ORDER BY id")
			require.NoError(test, err)
			_, err = node.DB.Exec("INSERT INTO " + sink.tablesClient.QualifiedTableName("t") + " VALUES (100, 1)")
			require.NoError(test, err)
			item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Table: "t", PartID: "buffered"}
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			item.Kind = abstract.InitTableLoad
			connection := testUploaderFixture(sink).connections[0]
			lostInsertReply := xerrors.New("injected lost final INSERT acknowledgement")
			if lostReply {
				connection.sendError = lostInsertReply
			}
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			rows := []abstract.ChangeItem{
				{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Table: "t", PartID: item.PartID, ColumnValues: []any{uint64(1), uint64(1)}},
				{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Table: "t", PartID: item.PartID, ColumnValues: []any{uint64(2), uint64(1)}},
				{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Table: "t", PartID: item.PartID, ColumnValues: []any{uint64(3), uint64(1)}},
			}
			require.NoError(test, sink.Push(rows[:1]))
			require.NoError(test, sink.Push(rows[1:]))
			require.Len(test, connection.batches, 1)
			batch := connection.batches[0]
			require.Zero(test, batch.sendCount)
			require.Len(test, batch.pendingRows, 3)
			resource := loadingPartTable(test, sink, item.TablePartID(), 0)
			rowCount, err := sink.tablesClient.CountPhysicalRows(ctx, node.Host.Name, resource.TableName)
			require.NoError(test, err)
			require.Zero(test, rowCount, "rows are buffered until the completion barrier")
			ready, err := readReadyAttempt(sink, ctx, item.TablePartID())
			require.NoError(test, err)
			require.Nil(test, ready)

			item.Kind = abstract.DoneTableLoad
			finishErr := sink.Push([]abstract.ChangeItem{item})
			require.Equal(test, 1, batch.sendCount)
			require.Empty(test, batch.pendingRows)
			if finishErr == nil {
				resource.TableName = strings.TrimSuffix(resource.TableName, "_loading") + "_ready"
			}
			rowCount, err = sink.tablesClient.CountPhysicalRows(ctx, node.Host.Name, resource.TableName)
			require.NoError(test, err)
			require.Equal(test, uint64(3), rowCount)
			ready, err = readReadyAttempt(sink, ctx, item.TablePartID())
			require.NoError(test, err)
			var expectedIDs []uint64
			if lostReply {
				require.ErrorIs(test, finishErr, lostInsertReply)
				require.Nil(test, ready, "a matching row count cannot replace the lost INSERT ACK")
				require.ErrorIs(test, sink.Push([]abstract.ChangeItem{item}), lostInsertReply)
				require.ErrorIs(test, sink.Push(rows), lostInsertReply)
				require.Error(test, sink.Commit(context.Background()))
				require.Equal(test, 1, batch.sendCount, "an uncertain INSERT must not be retried")
				var idsBeforeCleanup []uint64
				require.NoError(test, node.DB.QueryRow("SELECT arraySort(groupArray(id)) FROM "+sink.tablesClient.QualifiedTableName("t")).Scan(&idsBeforeCleanup))
				require.Equal(test, []uint64{100}, idsBeforeCleanup, "failed verification must not publish buffered data")
				recovery := newIntegrationWorker(test, sink, "cleanup-operation")
				require.NoError(test, recovery.Cleanup(ctx))
				expectedIDs = []uint64{100}
			} else {
				require.NoError(test, finishErr)
				require.NotNil(test, ready)
				_, readyRows := ready.Shard(0)
				require.EqualValues(test, 3, readyRows)
				require.NoError(test, sink.Commit(context.Background()))
				expectedIDs = []uint64{1, 2, 3, 100}
			}
			var actualIDs []uint64
			require.NoError(test, node.DB.QueryRow("SELECT arraySort(groupArray(id)) FROM "+sink.tablesClient.QualifiedTableName("t")).Scan(&actualIDs))
			require.Equal(test, expectedIDs, actualIDs)
			var temporaryTables uint64
			require.NoError(test, node.DB.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, '__dt_')", sink.dstConfig.Database).Scan(&temporaryTables))
			require.Zero(test, temporaryTables)
		})
	}
}

type unavailableHTTPClient struct {
	httpclient.HTTPClient
	unavailable           atomic.Bool
	losePublicationReply  atomic.Bool
	skipAssemblyAttach    atomic.Bool
	skippedAssemblyAttach atomic.Bool
}

func newUnavailableHTTPClient(client httpclient.HTTPClient) *unavailableHTTPClient {
	return &unavailableHTTPClient{HTTPClient: client, unavailable: atomic.Bool{}, losePublicationReply: atomic.Bool{}, skipAssemblyAttach: atomic.Bool{}, skippedAssemblyAttach: atomic.Bool{}}
}

func (c *unavailableHTTPClient) QueryStream(ctx context.Context, logger log.Logger, host *conn_clickhouse.Host, query any, params url.Values) (io.ReadCloser, error) {
	if c.unavailable.Load() {
		return nil, io.ErrUnexpectedEOF
	}
	statement := fmt.Sprint(query)
	if strings.HasPrefix(statement, "ALTER TABLE ") && strings.Contains(statement, "_assembly_") && strings.Contains(statement, " ATTACH PARTITION ") && c.skipAssemblyAttach.CompareAndSwap(true, false) {
		c.skippedAssemblyAttach.Store(true)
		return io.NopCloser(strings.NewReader("")), nil
	}
	response, err := c.HTTPClient.QueryStream(ctx, logger, host, query, params)
	if err != nil {
		return nil, err
	}
	if (strings.HasPrefix(statement, "EXCHANGE TABLES ") || strings.HasPrefix(statement, "RENAME TABLE ")) && c.losePublicationReply.CompareAndSwap(true, false) {
		_, _ = io.Copy(io.Discard, response)
		_ = response.Close()
		c.unavailable.Store(true)
		return nil, io.ErrUnexpectedEOF
	}
	return response, nil
}

func TestIntegrationReplicatedSnapshotUnavailableReplica(test *testing.T) {
	if os.Getenv("DT_SNAPSHOT_REPLICATED_DATABASE_TEST") != "1" {
		test.Skip("requires replicated ClickHouse fixture")
	}
	test.Setenv("DT_SNAPSHOT_CLICKHOUSE_TEST", "1")
	for _, scenario := range []string{"non_writer_unavailable_at_start", "writer_unavailable_after_assembly", "publication_reply_lost_with_initiator"} {
		test.Run(scenario, func(test *testing.T) {
			loseWriterAfterAssembly := scenario != "non_writer_unavailable_at_start"
			sink := newReplicatedDatabaseSink(test, model.Drop, true)
			sink.tablesClient.SetReplicaFailureGrace(time.Millisecond)
			if scenario == "publication_reply_lost_with_initiator" {
				primary := sink.tablesClient.NodesByShard()[0][0]
				_, err := primary.DB.Exec("CREATE TABLE " + sink.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE=ReplicatedMergeTree ORDER BY id")
				require.NoError(test, err)
				require.NoError(test, sink.databaseOperations.WaitForDatabaseDDL(context.Background()))
				_, err = primary.DB.Exec("INSERT INTO " + sink.tablesClient.QualifiedTableName("t") + " VALUES (99,1)")
				require.NoError(test, err)
			}
			failedIndex := 1
			if loseWriterAfterAssembly {
				failedIndex = 0
			}
			failedNode := &sink.tablesClient.NodesByShard()[0][failedIndex]
			client := newUnavailableHTTPClient(failedNode.HTTP)
			configureTestNodes(test, sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
				nodes[0][failedIndex].HTTP = client
				return nodes
			},
			)
			sink.tablesClient.SetReplicaFailureGrace(time.Millisecond)
			defer client.unavailable.Store(false)

			if !loseWriterAfterAssembly {
				client.unavailable.Store(true)
				sink.databaseOperations = nil
			}
			item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Table: "t", PartID: "part"}
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			item.Kind = abstract.InitTableLoad
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			require.NoError(test, sink.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Table: "t", PartID: "part", ColumnValues: []any{uint64(1), uint64(1)}}}))
			item.Kind = abstract.DoneTableLoad
			require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			if loseWriterAfterAssembly {
				tableState := defaultSnapshotTable(test, sink)
				sink.stopStagingDDLKeepalives()
				require.NoError(test, sink.closeTableStaging(ctx, tableState))
				attempts, err := part_uploader.DiscoverReady(ctx, sink.tablesClient, sink.operationsForTable(tableState), sink.dstConfig.Database, tableState.Snapshot())
				require.NoError(test, err)
				require.NoError(test, sink.buildCandidateTable(ctx, tableState, attempts))
				require.NoError(test, tableState.DecidePublication(ctx))
				if scenario == "publication_reply_lost_with_initiator" {
					client.losePublicationReply.Store(true)
				} else {
					client.unavailable.Store(true)
				}
				require.NoError(test, sink.publishAndCleanupTable(ctx, tableState))
				require.NoError(test, sink.Cleanup(ctx))
			} else {
				require.NoError(test, sink.Commit(ctx))
			}
			client.unavailable.Store(false)
			require.NoError(test, sink.databaseOperations.WaitForDatabaseDDL(ctx))
			for _, node := range sink.tablesClient.NodesByShard()[0] {
				require.NoError(test, sink.tablesClient.ExecuteCommandOverHTTP(ctx, node.Host.Name, "SYSTEM SYNC REPLICA "+sink.tablesClient.QualifiedTableName("t"), nil))
				var ids []uint64
				require.NoError(test, node.DB.QueryRow("SELECT groupArray(id) FROM "+sink.tablesClient.QualifiedTableName("t")).Scan(&ids))
				require.Equal(test, []uint64{1}, ids)
				var tmpCount uint64
				require.NoError(test, node.DB.QueryRow("SELECT count() FROM system.tables WHERE database=? AND startsWith(name,'__dt_')", sink.dstConfig.Database).Scan(&tmpCount))
				require.Zero(test, tmpCount)
			}
			operations, err := sink.publicationStore.ListTableStateKeysByOperation(context.Background())
			require.NoError(test, err)
			require.Empty(test, operations)
		})
	}
}

func TestIntegrationReplicatedSnapshotRebuildsIncompleteAssembly(test *testing.T) {
	if os.Getenv("DT_SNAPSHOT_REPLICATED_DATABASE_TEST") != "1" {
		test.Skip("requires replicated ClickHouse fixture")
	}
	test.Setenv("DT_SNAPSHOT_CLICKHOUSE_TEST", "1")
	sink := newReplicatedDatabaseSink(test, model.Drop, true)
	client := newUnavailableHTTPClient(sink.tablesClient.NodesByShard()[0][0].HTTP)
	configureTestNodes(test, sink, func(nodes [][]ch_eo_common.Node) [][]ch_eo_common.Node {
		nodes[0][0].HTTP = client

		return nodes
	},
	)
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Table: "t", PartID: "part"}
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	item.Kind = abstract.InitTableLoad
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	require.NoError(test, sink.Push([]abstract.ChangeItem{{Kind: abstract.InsertKind, TableSchema: snapshotTestSchema, ColumnNames: []string{"id", "version"}, Table: "t", PartID: "part", ColumnValues: []any{uint64(1), uint64(1)}}}))
	item.Kind = abstract.DoneTableLoad
	require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
	// A successful no-op ATTACH models a DDL executor with an empty source.
	// The real candidate must be discarded, not appended to again.
	client.skipAssemblyAttach.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	require.NoError(test, sink.Commit(ctx))
	require.True(test, client.skippedAssemblyAttach.Load())
	for _, node := range sink.tablesClient.NodesByShard()[0] {
		var ids []uint64
		require.NoError(test, node.DB.QueryRow("SELECT groupArray(id) FROM "+sink.tablesClient.QualifiedTableName("t")).Scan(&ids))
		require.Equal(test, []uint64{1}, ids)
		var tmpCount uint64
		require.NoError(test, node.DB.QueryRow("SELECT count() FROM system.tables WHERE database=? AND startsWith(name,'__dt_')", sink.dstConfig.Database).Scan(&tmpCount))
		require.Zero(test, tmpCount)
	}
}

// serialState models a remote JSON store, rather than retaining pointers into
// the running sink. Failed state writes must not leak through pointer aliases.
type serialState struct {
	coordinator.TransferState
	data map[string]json.RawMessage
}

func newSerialState() *serialState {
	return &serialState{
		TransferState: nil,
		data:          make(map[string]json.RawMessage),
	}
}

func (s *serialState) GetTransferStateKeys(string) ([]string, error) {
	var result []string
	for key := range s.data {
		result = append(result, key)
	}
	return result, nil
}

func (s *serialState) GetTransferStateByKeys(_ string, keys []string) (map[string]*coordinator.TransferStateData, error) {
	result := make(map[string]*coordinator.TransferStateData)
	for _, key := range keys {
		if raw, ok := s.data[key]; ok {
			var value coordinator.TransferStateData
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, err
			}
			result[key] = &value
		}
	}
	return result, nil
}

func (s *serialState) SetTransferState(_ string, values map[string]*coordinator.TransferStateData) error {
	for key, value := range values {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		s.data[key] = raw
	}
	return nil
}

func (s *serialState) RemoveTransferState(_ string, keys []string) error {
	for _, key := range keys {
		delete(s.data, key)
	}
	return nil
}

var snapshotTestSchema = abstract.NewTableSchema([]abstract.ColSchema{
	{ColumnName: "id", DataType: "uint64", Required: true},
	{ColumnName: "version", DataType: "uint64", Required: true},
})

type testUploadFixture struct {
	sharder     sharding.Sharder
	writeRows   func(ch_eo_common.Node, string, []abstract.ChangeItem) error
	appendRows  func(ch_eo_common.Node, string, []abstract.ChangeItem) error
	connections []*testUploadConnection
}

var _ driver.Conn = (*testUploadConnection)(nil)

type testUploadConnection struct {
	driver.Conn
	fixture      *testUploadFixture
	node         ch_eo_common.Node
	databaseName string
	batches      []*testUploadBatch
	sendError    error
}

func (c *testUploadConnection) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	qualifiedTable, found := strings.CutPrefix(query, "INSERT INTO "+ch_eo_common.QuoteIdentifier(c.databaseName)+".")
	if !found {
		return nil, xerrors.Errorf("unexpected test INSERT query: %s", query)
	}
	quotedTableName, _, found := strings.Cut(qualifiedTable, " (")
	if !found || !strings.HasPrefix(quotedTableName, "`") || !strings.HasSuffix(quotedTableName, "`") {
		return nil, xerrors.Errorf("unexpected test INSERT table: %s", query)
	}
	tableName := strings.ReplaceAll(quotedTableName[1:len(quotedTableName)-1], "``", "`")
	columns := make([]column.Interface, 0, 2)
	for _, columnName := range []string{"id", "version"} {
		targetColumn, err := column.Type("UInt64").Column(columnName, &column.ServerContext{Timezone: time.UTC})
		if err != nil {
			return nil, xerrors.Errorf("create test INSERT column: %w", err)
		}
		columns = append(columns, targetColumn)
	}
	batch := &testUploadBatch{connection: c, tableName: tableName, columns: columns}
	c.batches = append(c.batches, batch)
	return batch, nil
}

// Metadata connections belong to the integration fixture, not to the uploader.
func (c *testUploadConnection) Close() error { return nil }

var _ driver.Batch = (*testUploadBatch)(nil)

type testUploadBatch struct {
	driver.Batch
	connection  *testUploadConnection
	tableName   string
	columns     []column.Interface
	pendingRows []abstract.ChangeItem
	sendCount   int
	sent        bool
}

func (b *testUploadBatch) Columns() []column.Interface { return slices.Clone(b.columns) }

func (b *testUploadBatch) Append(arguments ...any) error {
	for columnIndex, argument := range arguments {
		if err := b.columns[columnIndex].AppendRow(argument); err != nil {
			return err
		}
	}
	// Native encoders reuse their arguments, and Push releases its grouped rows.
	// The driver's batch must own the data retained until the final Send.
	row := abstract.ChangeItem{
		Kind: abstract.InsertKind, TableSchema: snapshotTestSchema,
		ColumnNames: []string{"id", "version"}, ColumnValues: slices.Clone(arguments),
	}
	if b.connection.fixture.appendRows != nil {
		return b.connection.fixture.appendRows(b.connection.node, b.tableName, []abstract.ChangeItem{row})
	}
	b.pendingRows = append(b.pendingRows, row)
	return nil
}

func (b *testUploadBatch) Send() error {
	b.sendCount++
	b.sent = true
	rows := b.pendingRows
	b.pendingRows = nil
	if len(rows) != 0 {
		if err := b.connection.fixture.writeRows(b.connection.node, b.tableName, rows); err != nil {
			return err
		}
	}
	return b.connection.sendError
}

func (b *testUploadBatch) Flush() error {
	for _, targetColumn := range b.columns {
		targetColumn.Reset()
	}
	return nil
}

func (b *testUploadBatch) Abort() error {
	b.sent = true
	b.pendingRows = nil
	return nil
}

func (b *testUploadBatch) IsSent() bool { return b.sent }

func (b *testUploadBatch) Close() error { return b.Abort() }

var testUploadBySink sync.Map

func testUploaderFixture(sink *Sink) *testUploadFixture {
	fixture, _ := testUploadBySink.Load(sink)
	return fixture.(*testUploadFixture)
}

func newTestUploader(nodesByShard [][]ch_eo_common.Node, databaseName string, fixture *testUploadFixture) (*part_uploader.PartUploader, error) {
	uploader := part_uploader.NewUploader(len(nodesByShard), databaseName, false, nil, fixture.sharder)
	fixture.connections = make([]*testUploadConnection, len(nodesByShard))
	for shardIndex, nodes := range nodesByShard {
		connection := &testUploadConnection{fixture: fixture, node: nodes[0], databaseName: databaseName}
		fixture.connections[shardIndex] = connection
		if err := uploader.AddShard(shardIndex, connection, clickhouse_model.InsertParams{}.ToQueryOption(semver.MustParse("26.3.0"))); err != nil {
			_ = uploader.Close()
			return nil, err
		}
	}
	return uploader, nil
}

func rebuildTestUploader(test *testing.T, sink *Sink) {
	test.Helper()
	require.NoError(test, sink.uploader.Close())
	uploader, err := newTestUploader(sink.tablesClient.NodesByShard(), sink.dstConfig.Database, testUploaderFixture(sink))
	require.NoError(test, err)
	sink.uploader = uploader
}

func configureTestSharder(test *testing.T, sink *Sink, sharder sharding.Sharder) {
	test.Helper()
	testUploaderFixture(sink).sharder = sharder
	rebuildTestUploader(test, sink)
}

func setTestAppendRows(test *testing.T, sink *Sink, write func(ch_eo_common.Node, string, []abstract.ChangeItem) error) {
	test.Helper()
	fixture := testUploaderFixture(sink)
	fixture.writeRows = write
	fixture.appendRows = write
	rebuildTestUploader(test, sink)
}

func testWriteRows(sink *Sink) func(ch_eo_common.Node, string, []abstract.ChangeItem) error {
	return testUploaderFixture(sink).writeRows
}

func newTestSink(
	logger log.Logger,
	cfg *clickhouse_model.ChDestination,
	transferStateClient coordinator.TransferState,
	transferID, operationID string,
	nodesByShard [][]ch_eo_common.Node,
	useReplicatedEngine bool,
	sharder sharding.Sharder,
	buildTableName func(abstract.ChangeItem) string,
	buildDDL func(abstract.ChangeItem) (string, error),
	write func(ch_eo_common.Node, string, []abstract.ChangeItem) error,
) (*Sink, error) {
	fixture := &testUploadFixture{sharder: sharder, writeRows: write}
	uploader, err := newTestUploader(nodesByShard, cfg.Database, fixture)
	if err != nil {
		return nil, err
	}
	sink, err := newSink(context.Background(), logger, cfg, transferStateClient, transferID, operationID, nodesByShard, useReplicatedEngine, buildTableName, buildDDL, uploader)
	if err != nil {
		_ = uploader.Close()
		return nil, err
	}
	testStorageBySink.Store(sink, transferStateClient)
	testUploadBySink.Store(sink, fixture)
	return sink, nil
}

var testStorageBySink sync.Map

func testStateClient(sink *Sink) coordinator.TransferState {
	storage, _ := testStorageBySink.Load(sink)
	return storage.(coordinator.TransferState)
}

func TestIntegrationReusesStagingSessionsWithKeepalive(test *testing.T) {
	cleanupSink := newIntegrationSink(test, model.Drop, false)
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
	require.NoError(test, cleanupSink.Push([]abstract.ChangeItem{item}))
	cleanupSink.stopStagingDDLKeepalives()

	loadingSink := newIntegrationWorker(test, cleanupSink, cleanupSink.publicationStore.OperationID())
	require.Equal(test, 0, stagingDDLSessionCount(loadingSink))
	require.NoError(test, loadingSink.Push([]abstract.ChangeItem{item}))
	require.Equal(test, 1, stagingDDLSessionCount(loadingSink))
	// Repeated initialization must not create another keepalive or another gate.
	require.NoError(test, loadingSink.Push([]abstract.ChangeItem{item}))
	require.Equal(test, 1, stagingDDLSessionCount(loadingSink))
	require.NoError(test, loadingSink.Cleanup(context.Background()))
}

func stagingDDLSessionCount(sink *Sink) int {
	stagingDDLSessions.mutex.Lock()
	defer stagingDDLSessions.mutex.Unlock()
	operation := stagingDDLSessions.byOperation[sink.stagingDDLOperationKey(sink.publicationStore.OperationID())]
	if operation == nil {
		return 0
	}
	return len(operation.keepalives)
}

// The legacy loader closes its Init service sink before secondary workers start.
// The operation heartbeat must survive that Close, including the 60s idle timeout.
func TestIntegrationStagingSessionsSurviveServiceSinkClose(test *testing.T) {
	mainSink := newIntegrationSink(test, model.Drop, false)
	serviceSink := newIntegrationWorker(test, mainSink, mainSink.publicationStore.OperationID())
	// Give the short-lived service sink its own native connections, as production does.
	params, err := mainSink.dstConfig.ToSinkParams(&model.Transfer{})
	require.NoError(test, err)
	nodesByShard := mainSink.tablesClient.NodesByShard()
	for shardIndex := range nodesByShard {
		for nodeIndex := range nodesByShard[shardIndex] {
			node := &nodesByShard[shardIndex][nodeIndex]
			node.DB, err = conn.ConnectNative(node.Host, params)
			require.NoError(test, err)
		}
	}
	serviceSink.tablesClient = ch_eo_common.NewClient(mainSink.dstConfig, mainSink.logger, nodesByShard)
	item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t"}
	require.NoError(test, serviceSink.Push([]abstract.ChangeItem{item}))
	require.NoError(test, serviceSink.Close())
	require.Equal(test, 1, stagingDDLSessionCount(mainSink))
	require.NoError(test, ch_eo_common.WaitOrCancel(context.Background(), 65*time.Second))
	table, err := mainSink.publicationStore.LoadTable(context.Background(), mainSink.publicationStore.TableStateKey(item.TableID()))
	require.NoError(test, err)
	for hostName, sessionID := range table.Snapshot().StagingDDLSessionIDs {
		require.NoError(test, mainSink.tablesClient.ExecuteCommandOverHTTP(context.Background(), hostName, "SELECT 1 FORMAT Null", ch_eo_common.ExistingSessionParams(sessionID)))
	}
	require.NoError(test, mainSink.Push([]abstract.ChangeItem{{Kind: abstract.DoneShardedTableLoad, Schema: "public", Table: "t"}}))
	require.Zero(test, stagingDDLSessionCount(mainSink))
	require.NoError(test, mainSink.Cleanup(context.Background()))
}

// metrikaDDLBuilder supplies source DDL without an export API.
type metrikaDDLBuilder string

func (m metrikaDDLBuilder) IsMetrikaDDLBuilder() {}

func (m metrikaDDLBuilder) BuildClickHouseDDL(string, string, string, bool) (string, error) {
	return string(m), nil
}

func TestSnapshotDDLFromMetrikaSchema(test *testing.T) {
	definition := "CREATE TABLE ``.`` (Sign Int8, HitVersion UInt64) ENGINE = VersionedCollapsingMergeTree(Sign, HitVersion) ORDER BY HitVersion"
	fallbackCalls := 0
	sink := &Sink{destinationDDLFunc: func(abstract.ChangeItem) (string, error) {
		fallbackCalls++
		return "ordinary DDL", nil
	}}
	schema := abstract.NewTableSchema([]abstract.ColSchema{{ColumnName: "Sign", Properties: map[abstract.PropertyKey]interface{}{ddlbuilder.SnapshotDDLProperty: definition}}})
	item := abstract.ChangeItem{TableSchema: schema}
	actual, err := sink.snapshotDDL(item)
	require.NoError(test, err)
	require.Equal(test, definition, actual)
	require.Zero(test, fallbackCalls)
	for _, invalid := range []interface{}{nil, 1, ""} {
		schema.Columns()[0].Properties[ddlbuilder.SnapshotDDLProperty] = invalid
		actual, err = sink.snapshotDDL(item)
		require.Error(test, err)
		require.Empty(test, actual)
	}
	actual, err = sink.snapshotDDL(abstract.ChangeItem{})
	require.NoError(test, err)
	require.Equal(test, "ordinary DDL", actual)
	require.Equal(test, 1, fallbackCalls)
}

func TestMetrikaDDLEventDoesNotMutatePublicationState(test *testing.T) {
	ctx := context.Background()
	definition := "source DDL"
	store := transfer_state.NewState(newSerialState(), "transfer", "operation")
	tableID := abstract.TableID{Name: "hits_v2"}
	tableState := store.NewTable(tableID, "hits_v2", definition, false)
	tableState.AddReplica("host", 0, "")
	require.NoError(test, tableState.RegisterStaging(ctx, "prefix_", map[string]string{"host": "session"}))
	sink := &Sink{publicationStore: store, dstConfig: &clickhouse_model.ChDestination{Cleanup: model.Drop}}
	item := abstract.ChangeItem{Kind: abstract.ClickhouseDDLBuilderKind, Table: tableID.Name, ColumnValues: []interface{}{metrikaDDLBuilder(definition)}}
	require.NoError(test, sink.processItems(ctx, []abstract.ChangeItem{item, item}))
	storedTable, err := store.LoadTable(ctx, tableState.StateKey())
	require.NoError(test, err)
	require.Equal(test, tableState.Snapshot(), storedTable.Snapshot())
	item.ColumnValues = []interface{}{metrikaDDLBuilder("different DDL")}
	require.ErrorContains(test, sink.checkMetrikaDDL(ctx, item), "differs")
	item.ColumnValues = []interface{}{"not a builder"}
	require.ErrorContains(test, sink.checkMetrikaDDL(ctx, item), "invalid builder")
	item.ColumnValues = nil
	require.ErrorContains(test, sink.checkMetrikaDDL(ctx, item), "one builder")
	item.ColumnValues = []interface{}{metrikaDDLBuilder(definition)}
	item.Table = "unknown"
	require.ErrorContains(test, sink.checkMetrikaDDL(ctx, item), "not open")
	item.Table = tableID.Name
	require.NoError(test, tableState.CloseStaging(ctx))
	require.ErrorContains(test, sink.checkMetrikaDDL(ctx, item), "not open")
}
