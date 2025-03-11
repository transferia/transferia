package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	chrecipe "github.com/transferria/transferria/pkg/providers/clickhouse/recipe"
	pgcommon "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump/pg"))
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

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	time.Sleep(5 * time.Second) // for the worker to start

	_, err = conn.Exec(context.Background(), "INSERT INTO test (id, output, input) VALUES (3, 'c', 'something'), (4, 'd', 'something else')")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "UPDATE test SET output = 'test_update' WHERE id = 1")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "DELETE FROM test WHERE id = 2")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
}
