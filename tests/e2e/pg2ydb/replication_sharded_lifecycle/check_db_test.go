package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	_ "github.com/transferia/transferia/tests/helpers/registration/ydb"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

// This test covers the replication path with YDB destination sharding
// (ShardCount > 0, `_shard_key` is prepended to the target primary key).
// It reproduces insert -> update -> delete for a single row and checks that:
//   - delete does not panic (the original bug: `_shard_key` was computed from
//     ColumnValues[0], which is empty for DeleteKind change items);
//   - update does not leave a stray duplicate row behind, which would happen
//     if the `_shard_key` computed for the update differs from the one
//     stored by the preceding insert;
//   - delete actually removes the row, which would silently match nothing if
//     its `_shard_key` differs from the one stored on disk.
var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	tableName    = "items"
)

func TestReplicationShardedLifecycle(t *testing.T) {
	Source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	Target := &provider_ydb.YdbDestination{
		Token:      model.SecretString(os.Getenv("YDB_TOKEN")),
		Database:   testenv.GetEnvOfFail(t, "YDB_DATABASE"),
		Instance:   testenv.GetEnvOfFail(t, "YDB_ENDPOINT"),
		ShardCount: 4,
	}

	t.Setenv("YC", "1")                                                                  // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable

	defer func() {
		sourcePort, err := network.GetPortFromStr(Target.Instance)
		require.NoError(t, err)
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "YDB target", Port: sourcePort},
		))
	}()

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, Target, TransferType)
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	conn, err := provider_postgres.MakeConnPoolFromSrc(Source, logger.Log)
	require.NoError(t, err)

	sourceStorage := storagecomparison.GetSampleableStorageByModel(t, Source)
	targetStorage := storagecomparison.GetSampleableStorageByModel(t, Target)

	initialRowsCount, err := sourceStorage.ExactTableRowsCount(*abstract.NewTableID(databaseName, tableName))
	require.NoError(t, err)
	require.NoError(t, storage.WaitEqualRowsCount(t, databaseName, tableName, sourceStorage, targetStorage, 60*time.Second))

	// INSERT: a fresh row must reach the destination.
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`insert into %s values(100, 'v1')`, tableName))
	require.NoError(t, err)
	require.NoError(t, storage.WaitEqualRowsCount(t, databaseName, tableName, sourceStorage, targetStorage, 60*time.Second))

	// UPDATE: if the sharding key computed for the update does not match the
	// one stored by insert, it would land as a brand new row with a
	// different `_shard_key` instead of overwriting the existing one, and
	// the row count would stay above initialRowsCount+1 forever.
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`update %s set val = 'v2' where id = 100`, tableName))
	require.NoError(t, err)
	require.NoError(t, storage.WaitDestinationEqualRowsCount(databaseName, tableName, targetStorage, 60*time.Second, initialRowsCount+1))

	// DELETE: on DeleteKind change items ColumnValues is empty, so computing
	// `_shard_key` from a fixed position used to panic; now it is computed
	// from OldKeys and must match the value stored by insert/update, or the
	// delete will silently match zero rows and the row will stay behind.
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`delete from %s where id = 100`, tableName))
	require.NoError(t, err)
	require.NoError(t, storage.WaitEqualRowsCount(t, databaseName, tableName, sourceStorage, targetStorage, 60*time.Second))

	require.NoError(t, storage.WaitDestinationEqualRowsCount(databaseName, tableName, targetStorage, 60*time.Second, initialRowsCount))
}
