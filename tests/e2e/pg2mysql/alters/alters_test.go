package alters

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	pg_provider "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("pg_source"))
	Target       = *helpers.RecipeMysqlTarget()
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestAlter(t *testing.T) {
	time.Sleep(5 * time.Second)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "MYSQL target", Port: Target.Port},
		))
	}()
	Target.MaintainTables = false
	conn, err := pg_provider.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer conn.Close()

	//------------------------------------------------------------------------------------
	// start worker

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	var terminateErr error
	localWorker := helpers.Activate(t, transfer, func(err error) {
		terminateErr = err
	})
	defer localWorker.Close(t)

	t.Run("ADD COLUMN", func(t *testing.T) {
		_, err := conn.Exec(context.Background(), "INSERT INTO __test (id, val1, val2) VALUES (6, 6, 'c')")
		require.NoError(t, err)

		time.Sleep(10 * time.Second)

		_, err = conn.Exec(context.Background(), "ALTER TABLE __test ADD COLUMN new_val INTEGER")
		require.NoError(t, err)

		time.Sleep(10 * time.Second)

		rows, err := conn.Query(context.Background(), "INSERT INTO __test (id, val1, val2, new_val) VALUES (7, 7, 'd', 7)")
		require.NoError(t, err)
		rows.Close()

		//------------------------------------------------------------------------------------
		// wait & compare

		require.NoError(t, helpers.WaitDestinationEqualRowsCount(
			Target.Database,
			"__test",
			helpers.GetSampleableStorageByModel(t, Target),
			60*time.Second,
			4,
		))
	})

	require.NoError(t, terminateErr)
}
