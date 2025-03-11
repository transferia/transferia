package replication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	cpclient "github.com/transferria/transferria/pkg/abstract/coordinator"
	chrecipe "github.com/transferria/transferria/pkg/providers/clickhouse/recipe"
	pgcommon "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/pkg/runtime/local"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"), pgrecipe.WithPrefix(""))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
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

	rows, err := conn.Query(context.Background(), "INSERT INTO public.date_types (__primary_key) VALUES (default)")
	require.NoError(t, err)
	rows.Close()

	rows, err = conn.Query(context.Background(), "UPDATE public.date_types SET t_time=now() WHERE __primary_key=2;")
	require.NoError(t, err)
	rows.Close()

	rows, err = conn.Query(context.Background(), "UPDATE public.date_types SET t_time=null WHERE __primary_key=2;")
	require.NoError(t, err)
	rows.Close()

	//------------------------------------------------------------------------------------
	// wait & compare

	require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "date_types", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
}
