package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/e2e/pg2ch"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"), pgrecipe.WithPrefix(""))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("YC", "1")                                                               // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	Target.Cleanup = model.DisabledCleanup

	connConfig, err := provider_postgres.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	conn, err := provider_postgres.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)

	err = tasks.ActivateDelivery(context.Background(), nil, coordinator.NewFakeClient(), *transfer, testmetrics.EmptyRegistry())
	require.Error(t, err) // MatView failed

	Target.InsertParams = clickhouse_model.InsertParams{MaterializedViewsIgnoreErrors: true}
	err = tasks.ActivateDelivery(context.Background(), nil, coordinator.NewFakeClient(), *transfer, testmetrics.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, testmetrics.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// insert/update/delete several record

	rows, err := conn.Query(context.Background(), "INSERT INTO __test (id, val1, val2) VALUES (3, 3, 'c'), (4, 4, 'd'), (5, 5, 'e')")
	require.NoError(t, err)
	rows.Close()

	rows, err = conn.Query(context.Background(), "UPDATE __test SET val1=22 WHERE id=2;")
	require.NoError(t, err)
	rows.Close()

	rows, err = conn.Query(context.Background(), "DELETE FROM __test WHERE id=3;")
	require.NoError(t, err)
	rows.Close()

	//------------------------------------------------------------------------------------
	// wait & compare

	require.NoError(t, storage.WaitEqualRowsCount(t, databaseName, "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))

	//------------------------------------------------------------------------------------
	// check DELETE + INSERT case
	_, err = conn.Exec(context.Background(), "INSERT INTO __test (id, val1, val2) VALUES (10, 1, 'attempt1')")
	require.NoError(t, err)

	tctx := context.Background()
	tx, err := conn.Begin(tctx)
	require.NoError(t, err)
	_, err = tx.Exec(tctx, "DELETE FROM __test WHERE id = 10")
	require.NoError(t, err)
	_, err = tx.Exec(tctx, "INSERT INTO __test (id, val1, val2) VALUES (10, 2, 'attempt2')")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(tctx))

	require.NoError(t, storage.WaitEqualRowsCount(t, databaseName, "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
}
