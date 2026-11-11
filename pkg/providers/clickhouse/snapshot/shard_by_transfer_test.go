package snapshot

import (
	"context"
	"fmt"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/nop"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
)

func TestIntegrationShardByTransferIDPreservesOtherShard(test *testing.T) {
	for _, policy := range []model.CleanupType{model.Drop, model.Truncate} {
		for _, transferID := range []string{"transfer-a", "transfer-b"} {
			test.Run(fmt.Sprintf("%s/%s", policy, transferID), func(test *testing.T) {
				fixture := newTwoShardAtomicIntegrationSink(test)
				previousResolver := connection.Resolver()
				test.Cleanup(func() { connection.Init(previousResolver) })
				resolver := connection.NewStubConnectionResolver()
				hosts := make([]*conn_clickhouse.Host, 0, 2)
				for shardIndex, nodes := range fixture.tablesClient.NodesByShard() {
					host := *nodes[0].Host
					host.ShardName = fmt.Sprintf("s%d", shardIndex)
					hosts = append(hosts, &host)
				}
				managedConnection := new(conn_clickhouse.Connection)
				managedConnection.Hosts = hosts
				managedConnection.User = "default"
				managedConnection.ShardGroups = map[string][]string{"snapshot-shards": {"s0", "s1"}}
				require.NoError(test, resolver.Add("snapshot-shards", managedConnection))
				connection.Init(resolver)
				destination := *fixture.dstConfig
				destination.ConnectionID = "snapshot-shards"
				destination.ChClusterName = "snapshot-shards"
				destination.ShardByTransferID = true
				destination.ExactlyOnceSnapshot = true
				destination.Cleanup = policy
				transfer := &model.Transfer{ID: transferID, Dst: &destination, Type: abstract.TransferTypeSnapshotOnly, Runtime: &abstract.LocalRuntime{}}
				params, err := destination.ToSinkParams(transfer)
				require.NoError(test, err)
				selectedShard := int(sharding.CHSharder(params, transferID)(abstract.ChangeItem{}))
				previousUUIDs := make([]string, len(hosts))
				for shardIndex, nodes := range fixture.tablesClient.NodesByShard() {
					_, err := nodes[0].DB.Exec("CREATE TABLE " + fixture.tablesClient.QualifiedTableName("t") + " (id UInt64, version UInt64) ENGINE=MergeTree ORDER BY id")
					require.NoError(test, err)
					_, err = nodes[0].DB.Exec("INSERT INTO " + fixture.tablesClient.QualifiedTableName("t") + fmt.Sprintf(" VALUES (%d,1)", 900+shardIndex))
					require.NoError(test, err)
					previousUUIDs[shardIndex], err = fixture.tablesClient.TableUUID(context.Background(), nodes[0].Host.Name, "t")
					require.NoError(test, err)
				}
				sink, err := NewSinkFromTransfer(
					context.Background(), fixture.logger, nop.Registry{}, newSerialState(), transfer, "snapshot-operation", false,
					func(clickhouse_model.ChSinkParams, semver.Version) (func(abstract.ChangeItem) string, func(abstract.ChangeItem) (string, error)) {
						return fixture.destinationTableNameFunc, fixture.destinationDDLFunc
					},
				)
				require.NoError(test, err)
				test.Cleanup(func() {
					require.NoError(test, sink.Close())
					StopStagingDDLKeepalives(transferID, "snapshot-operation")
				})
				item := abstract.ChangeItem{Kind: abstract.InitShardedTableLoad, Schema: "public", Table: "t", PartID: "part", TableSchema: snapshotTestSchema}
				require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
				item.Kind = abstract.InitTableLoad
				require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
				item.Kind = abstract.InsertKind
				item.ColumnNames = []string{"id", "version"}
				item.ColumnValues = []any{uint64(1), uint64(1)}
				require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
				item.Kind = abstract.DoneTableLoad
				require.NoError(test, sink.Push([]abstract.ChangeItem{item}))
				require.NoError(test, sink.Commit(context.Background()))
				for shardIndex, nodes := range fixture.tablesClient.NodesByShard() {
					var ids []uint64
					require.NoError(test, nodes[0].DB.QueryRow("SELECT arraySort(groupArray(id)) FROM "+fixture.tablesClient.QualifiedTableName("t")).Scan(&ids))
					if shardIndex == selectedShard {
						require.Equal(test, []uint64{1}, ids)
					} else {
						require.Equal(test, []uint64{uint64(900 + shardIndex)}, ids, "another transfer's shard must retain its rows")
						uuid, err := fixture.tablesClient.TableUUID(context.Background(), nodes[0].Host.Name, "t")
						require.NoError(test, err)
						require.Equal(test, previousUUIDs[shardIndex], uuid)
					}
				}
			})
		}
	}
}
