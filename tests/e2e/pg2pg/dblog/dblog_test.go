package dblog

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/dblog"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	postgres_dblog "github.com/transferia/transferia/pkg/providers/postgres/dblog"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"), pgrecipe.WithDBTables("public.__test"))
	Target       = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
	ctx          = context.Background()
)

func init() {
	_ = os.Setenv("YC", "1")                                                               // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	Source.DBLogEnabled = true
	Source.ChunkSize = 2
}

func TestDBLog(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 240*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	// after all the data has been copied from the source code, all kinds of watermarks are expected
	checkAllWatermarks(t, srcConn, true)

	dstConn, err := provider_postgres.MakeConnPoolFromDst(&Target, logger.Log)
	require.NoError(t, err)
	defer dstConn.Close()

	// check replication
	_, err = srcConn.Exec(ctx, "INSERT INTO __test VALUES('11', '11');")
	require.NoError(t, err)
	_, err = srcConn.Exec(ctx, "INSERT INTO __test VALUES('12', '12');")
	require.NoError(t, err)
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 240*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
	worker.Close(t)

	// if success watermark is not removed this row will not be transfered after the restart
	_, err = srcConn.Exec(ctx, "INSERT INTO __test VALUES('-1', '-1');")
	require.NoError(t, err)

	worker.Restart(t, transfer)
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 30*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))

	require.NoError(t, postgres_dblog.DeleteWatermarks(ctx, srcConn, Source.KeeperSchema, transfer.ID))
	checkAllWatermarks(t, srcConn, false)
}

func checkWatermarkExist(t *testing.T, mark dblog.WatermarkType, srcConn *pgxpool.Pool, expectedExist bool) {
	var hasWatermark bool
	err := srcConn.QueryRow(ctx, fmt.Sprintf("SELECT EXISTS (SELECT true FROM %s WHERE mark_type = ($1));", postgres_dblog.SignalTableName), mark).Scan(&hasWatermark)
	require.Equal(t, expectedExist, hasWatermark)
	require.NoError(t, err)
}

func checkAllWatermarks(t *testing.T, srcConn *pgxpool.Pool, expectedExist bool) {
	checkWatermarkExist(t, dblog.LowWatermarkType, srcConn, expectedExist)
	checkWatermarkExist(t, dblog.HighWatermarkType, srcConn, expectedExist)
	checkWatermarkExist(t, dblog.SuccessWatermarkType, srcConn, expectedExist)
}
