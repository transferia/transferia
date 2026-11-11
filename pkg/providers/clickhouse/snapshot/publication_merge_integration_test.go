package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/nop"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/part_uploader"
	"github.com/transferia/transferia/pkg/stats"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func newNativeMergeIntegrationWorker(test *testing.T, source *Sink) *Sink {
	test.Helper()
	node := source.tablesClient.NodesByShard()[0][0]
	options := new(clickhouse_go.Options)
	options.Addr = []string{fmt.Sprintf("%s:%d", node.Host.Name, node.Host.NativePort)}
	options.Auth.Database = source.dstConfig.Database
	options.Auth.Username = source.dstConfig.User
	options.Settings = clickhouse_go.Settings{"async_insert": 0, "insert_deduplicate": 0}
	connection, err := part_uploader.OpenConnection(options)
	require.NoError(test, err)
	uploader := part_uploader.NewUploader(
		1, source.dstConfig.Database, false,
		stats.NewChStats(nop.Registry{}), sharding.ConstSharder(),
	)
	require.NoError(test, uploader.AddShard(0, connection, clickhouse_go.WithSettings(options.Settings)))
	test.Cleanup(func() { require.NoError(test, uploader.Close()) })
	worker, err := newSink(
		context.Background(),
		source.logger,
		source.dstConfig,
		testStateClient(source),
		source.transferID,
		source.publicationStore.OperationID(),
		source.tablesClient.NodesByShard(),
		false,
		source.destinationTableNameFunc,
		source.destinationDDLFunc,
		uploader,
	)
	require.NoError(test, err)
	return worker
}

func TestIntegrationPublicationResumesAfterEngineMerge(test *testing.T) {
	for _, testCase := range []struct {
		name            string
		ddl             string
		schema          *abstract.TableSchema
		rows            [][]any
		expectedRows    uint64
		expectedVersion uint64
	}{
		{
			name:            "replacing",
			ddl:             "CREATE TABLE t (id UInt64, version UInt64) ENGINE = ReplacingMergeTree(version) ORDER BY id",
			schema:          snapshotTestSchema,
			rows:            [][]any{{uint64(1), uint64(1)}, {uint64(1), uint64(2)}},
			expectedRows:    1,
			expectedVersion: 2,
		},
		{
			name: "versioned_collapsing",
			ddl:  "CREATE TABLE t (id UInt64, version UInt64, Sign Int8) ENGINE = VersionedCollapsingMergeTree(Sign, version) ORDER BY id",
			schema: abstract.NewTableSchema([]abstract.ColSchema{
				abstract.NewColSchema("id", ytschema.TypeUint64, true),
				abstract.NewColSchema("version", ytschema.TypeUint64, false),
				abstract.NewColSchema("Sign", ytschema.TypeInt8, false),
			}),
			rows:            [][]any{{uint64(1), uint64(2), int8(1)}, {uint64(1), uint64(2), int8(-1)}},
			expectedRows:    0,
			expectedVersion: 0,
		},
	} {
		for _, published := range []bool{false, true} {
			test.Run(fmt.Sprintf("%s/published=%t", testCase.name, published), func(test *testing.T) {
				sink := newIntegrationSink(test, model.Drop, false)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				sink.destinationDDLFunc = func(abstract.ChangeItem) (string, error) { return testCase.ddl, nil }
				control := new(abstract.ChangeItem)
				control.Kind = abstract.InitShardedTableLoad
				control.Schema = "public"
				control.Table = "t"
				require.NoError(test, sink.Push([]abstract.ChangeItem{*control}))
				for partIndex, partID := range []string{"first", "second"} {
					worker := newNativeMergeIntegrationWorker(test, sink)
					control.Kind = abstract.InitTableLoad
					control.PartID = partID
					require.NoError(test, worker.Push([]abstract.ChangeItem{*control}))
					row := new(abstract.ChangeItem)
					row.Kind = abstract.InsertKind
					row.Schema = control.Schema
					row.Table = control.Table
					row.PartID = partID
					row.TableSchema = testCase.schema
					row.ColumnNames = testCase.schema.Columns().ColumnNames()
					row.ColumnValues = testCase.rows[partIndex]
					require.NoError(test, worker.Push([]abstract.ChangeItem{*row}))
					control.Kind = abstract.DoneTableLoad
					require.NoError(test, worker.Push([]abstract.ChangeItem{*control}))
					require.NoError(test, worker.uploader.Close())
				}
				table := loadSnapshotTable(test, sink, control.TableID())
				require.NoError(test, sink.closeTableStaging(ctx, table))
				parts := readyParts(test, sink, table)
				require.NoError(test, sink.buildCandidateTable(ctx, table, parts))
				require.NoError(test, table.DecidePublication(ctx))
				candidate := table.Replicas()[0].CandidateTable
				db := sink.tablesClient.NodesByShard()[0][0].DB
				mergedTableName := candidate.TableName
				if published {
					_, err := db.ExecContext(ctx, "RENAME TABLE "+sink.tablesClient.QualifiedTableName(candidate.TableName)+" TO "+sink.tablesClient.QualifiedTableName("t"))
					require.NoError(test, err)
					mergedTableName = "t"
				}
				// The build must already have resumed normal merges before its decision.
				_, err := db.ExecContext(ctx, "OPTIMIZE TABLE "+sink.tablesClient.QualifiedTableName(mergedTableName)+" FINAL")
				require.NoError(test, err)
				rows, err := sink.tablesClient.CountPhysicalRows(ctx, candidate.CommandHostName, mergedTableName)
				require.NoError(test, err)
				require.Equal(test, testCase.expectedRows, rows)
				// Recovery must use the verified candidate even if source parts are gone.
				for _, part := range parts {
					partTable, _ := part.Shard(0)
					_, err := db.ExecContext(ctx, "DROP TABLE "+sink.tablesClient.QualifiedTableName(partTable.TableName)+" SYNC")
					require.NoError(test, err)
				}
				recovery := newIntegrationWorker(test, sink, sink.publicationStore.OperationID())
				require.NoError(test, recovery.Commit(ctx))
				publishedUUID, err := recovery.tablesClient.TableUUID(ctx, candidate.CommandHostName, "t")
				require.NoError(test, err)
				require.Equal(test, candidate.ExpectedTableUUID, publishedUUID)
				var count, version uint64
				require.NoError(test, db.QueryRowContext(ctx, "SELECT count(), max(version) FROM "+sink.tablesClient.QualifiedTableName("t")).Scan(&count, &version))
				require.Equal(test, testCase.expectedRows, count)
				require.Equal(test, testCase.expectedVersion, version)
				keys, err := testStateClient(sink).GetTransferStateKeys(sink.transferID)
				require.NoError(test, err)
				require.Empty(test, keys)
			})
		}
	}
}

func TestIntegrationVersionedCollapsingSnapshotPreservesVersionOrdering(test *testing.T) {
	for _, databaseEngine := range []string{"Atomic", "Replicated"} {
		test.Run(databaseEngine, func(test *testing.T) {
			var sink *Sink
			if databaseEngine == "Replicated" {
				if os.Getenv("DT_SNAPSHOT_REPLICATED_DATABASE_TEST") != "1" {
					test.Skip("requires two ClickHouse servers with a shared Keeper")
				}
				sink = newReplicatedDatabaseSink(test, model.Drop, true)
			} else {
				sink = newIntegrationSink(test, model.Drop, false)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			sink.destinationDDLFunc = func(abstract.ChangeItem) (string, error) {
				return "CREATE TABLE t (id UInt64, version UInt64, Sign Int8) ENGINE=VersionedCollapsingMergeTree(Sign, version) ORDER BY id", nil
			}
			schema := abstract.NewTableSchema([]abstract.ColSchema{
				abstract.NewColSchema("id", ytschema.TypeUint64, true),
				abstract.NewColSchema("version", ytschema.TypeUint64, false),
				abstract.NewColSchema("Sign", ytschema.TypeInt8, false),
			})
			control := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Table: "t", PartID: "unordered-versions"}
			require.NoError(test, sink.Push([]abstract.ChangeItem{control}))
			worker := newNativeMergeIntegrationWorker(test, sink)
			control.Kind = abstract.InitTableLoad
			require.NoError(test, worker.Push([]abstract.ChangeItem{control}))
			rows := make([]abstract.ChangeItem, 0, 100)
			// Equal declared keys arrive in version order 0, 5, ..., 1, 6, ... .
			// The snapshot contains only positive signs, so no rows can collapse.
			for remainder := uint64(0); remainder < 5; remainder++ {
				for version := remainder; version < 100; version += 5 {
					rows = append(rows, abstract.ChangeItem{
						Kind:         abstract.InsertKind,
						Table:        control.Table,
						PartID:       control.PartID,
						TableSchema:  schema,
						ColumnNames:  schema.Columns().ColumnNames(),
						ColumnValues: []any{uint64(1), version, int8(1)},
					})
				}
			}
			require.NoError(test, worker.Push(rows))
			control.Kind = abstract.DoneTableLoad
			require.NoError(test, worker.Push([]abstract.ChangeItem{control}))
			require.NoError(test, worker.uploader.Close())
			require.NoError(test, sink.Commit(ctx))
			primary := sink.tablesClient.NodesByShard()[0][0]
			physicalRows, err := sink.tablesClient.CountPhysicalRows(ctx, primary.Host.Name, "t")
			require.NoError(test, err)
			require.EqualValues(test, 100, physicalRows)
			var snapshotRows uint64
			require.NoError(test, primary.DB.QueryRowContext(ctx, "SELECT count() FROM "+sink.tablesClient.QualifiedTableName("t")+" FINAL").Scan(&snapshotRows))
			require.EqualValues(test, 100, snapshotRows)
			// Ordinary inserts sort their parts by the engine's effective key.
			// Every published version must pair with its later inverse row.
			_, err = primary.DB.ExecContext(ctx, "INSERT INTO "+sink.tablesClient.QualifiedTableName("t")+" SELECT 1, number, -1 FROM numbers(100)")
			require.NoError(test, err)
			for _, node := range sink.tablesClient.NodesByShard()[0] {
				if databaseEngine == "Replicated" {
					require.NoError(test, sink.tablesClient.ExecuteCommandOverHTTP(ctx, node.Host.Name, "SYSTEM SYNC REPLICA "+sink.tablesClient.QualifiedTableName("t"), nil))
				}
				var remainingRows uint64
				require.NoError(test, node.DB.QueryRowContext(ctx, "SELECT count() FROM "+sink.tablesClient.QualifiedTableName("t")+" FINAL").Scan(&remainingRows))
				require.Zero(test, remainingRows, "snapshot versions must cancel after publication on %s", node.Host.Name)
			}
		})
	}
}

func TestIntegrationVersionedCollapsingSnapshotPreservesPhysicalRowsDuringCopy(test *testing.T) {
	for _, databaseEngine := range []string{"Atomic", "Replicated"} {
		test.Run(databaseEngine, func(test *testing.T) {
			var sink *Sink
			if databaseEngine == "Replicated" {
				if os.Getenv("DT_SNAPSHOT_REPLICATED_DATABASE_TEST") != "1" {
					test.Skip("requires two ClickHouse servers with a shared Keeper")
				}
				sink = newReplicatedDatabaseSink(test, model.Drop, true)
			} else {
				sink = newIntegrationSink(test, model.Drop, false)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			sink.destinationDDLFunc = func(abstract.ChangeItem) (string, error) {
				return "CREATE TABLE t (id UInt64, version UInt64, Sign Int8) ENGINE=VersionedCollapsingMergeTree(Sign, version) ORDER BY id", nil
			}
			schema := abstract.NewTableSchema([]abstract.ColSchema{
				abstract.NewColSchema("id", ytschema.TypeUint64, true),
				abstract.NewColSchema("version", ytschema.TypeUint64, false),
				abstract.NewColSchema("Sign", ytschema.TypeInt8, false),
			})
			control := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Table: "t", PartID: "opposite-signs"}
			require.NoError(test, sink.Push([]abstract.ChangeItem{control}))
			worker := newNativeMergeIntegrationWorker(test, sink)
			control.Kind = abstract.InitTableLoad
			require.NoError(test, worker.Push([]abstract.ChangeItem{control}))
			rows := make([]abstract.ChangeItem, 0, 2)
			for _, sign := range []int8{1, -1} {
				rows = append(rows, abstract.ChangeItem{
					Kind:         abstract.InsertKind,
					Table:        control.Table,
					PartID:       control.PartID,
					TableSchema:  schema,
					ColumnNames:  schema.Columns().ColumnNames(),
					ColumnValues: []any{uint64(1), uint64(1), sign},
				})
			}
			require.NoError(test, worker.Push(rows))
			control.Kind = abstract.DoneTableLoad
			require.NoError(test, worker.Push([]abstract.ChangeItem{control}))
			require.NoError(test, worker.uploader.Close())
			// The sorting copy must preserve both physical rows until validation.
			// Collapsing is allowed again once the complete candidate is recorded.
			require.NoError(test, sink.Commit(ctx))
			for _, node := range sink.tablesClient.NodesByShard()[0] {
				var remainingRows uint64
				require.NoError(test, node.DB.QueryRowContext(ctx, "SELECT count() FROM "+sink.tablesClient.QualifiedTableName("t")+" FINAL").Scan(&remainingRows))
				require.Zero(test, remainingRows)
			}
		})
	}
}
