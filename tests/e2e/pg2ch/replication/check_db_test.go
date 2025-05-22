package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/clickhouse"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/e2e/pg2ch"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"), pgrecipe.WithPrefix(""))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	connConfig, err := pgcommon.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	conn, err := pgcommon.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	err = tasks.ActivateDelivery(context.Background(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
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

	require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))

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

	require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
}

func TestOptimizeCleanup(t *testing.T) {
	// Setup same as in TestSnapshotAndIncrement
	connConfig, err := pgcommon.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	conn, err := pgcommon.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	// Start transfer
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	err = tasks.ActivateDelivery(context.Background(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop()

	// Insert test data
	rows, err := conn.Query(context.Background(), "INSERT INTO __test (id, val1, val2) VALUES (100, 100, 'test_cleanup')")
	require.NoError(t, err)
	rows.Close()

	// Wait until data appears in CH
	require.NoError(
		t,
		helpers.WaitEqualRowsCount(
			t,
			databaseName,
			"__test",
			helpers.GetSampleableStorageByModel(t, Source),
			helpers.GetSampleableStorageByModel(t, Target),
			60*time.Second,
		),
	)

	// Delete the data
	rows, err = conn.Query(context.Background(), "DELETE FROM __test WHERE id=100")
	require.NoError(t, err)
	rows.Close()

	// Wait until deletion is reflected in CH
	require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "__test",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target),
		60*time.Second))

	// Get CH connection for verification
	storageParams, err := Target.ToStorageParams()
	require.NoError(t, err)
	chConn, err := clickhouse.MakeConnection(storageParams)
	require.NoError(t, err)

	// Run OPTIMIZE ... FINAL CLEANUP
	_, err = chConn.Exec("OPTIMIZE TABLE public.__test FINAL CLEANUP")
	require.NoError(t, err)

	// Verify that rows are physically deleted
	var count int
	err = chConn.QueryRow("SELECT count() FROM public.__test WHERE id = 100").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	// Verify that rows don't reappear after OPTIMIZE
	err = chConn.QueryRow("SELECT count() FROM public.__test FINAL WHERE id = 100").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}
