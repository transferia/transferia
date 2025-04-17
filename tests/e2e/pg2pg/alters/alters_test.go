package alters

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"))
	Target       = *pgrecipe.RecipeTarget()
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
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	connConfig, err := pgcommon.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	conn, err := pgcommon.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker
	Target.MaintainTables = true
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	var terminateErr error
	localWorker := helpers.Activate(t, transfer, func(err error) {
		terminateErr = err
	})
	defer localWorker.Close(t)

	t.Run("ADD COLUMN", func(t *testing.T) {
		rows, err := conn.Query(context.Background(), `INSERT INTO __test (id, "Val1", val2) VALUES (6, 6, 'c')`)
		require.NoError(t, err)
		rows.Close()

		require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))

		//require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))

		rows, err = conn.Query(context.Background(), "ALTER TABLE __test ADD COLUMN new_val INTEGER")
		require.NoError(t, err)
		rows.Close()

		time.Sleep(10 * time.Second)

		rows, err = conn.Query(context.Background(), `INSERT INTO __test (id, "Val1", val2, new_val) VALUES (7, 7, 'd', 7)`)
		require.NoError(t, err)
		rows.Close()

		//------------------------------------------------------------------------------------
		// wait & compare
		require.NoError(t, helpers.WaitStoragesSynced(t, Source, Target, 50, helpers.NewCompareStorageParams()))
	})

	t.Run("ADD COLUMN single transaction", func(t *testing.T) {
		// force INSERTs with different schemas to be pushed with one ApplyChangeItems call
		err := conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
			rows, err := tx.Query(context.Background(), `INSERT INTO __test (id, "Val1", val2) VALUES (8, 8, 'e')`)
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "ALTER TABLE __test ADD COLUMN new_val2 INTEGER")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), `INSERT INTO __test (id, "Val1", val2, new_val2) VALUES (9, 9, 'f', 9)`)
			require.NoError(t, err)
			rows.Close()
			return nil
		})
		require.NoError(t, err)

		//------------------------------------------------------------------------------------
		// wait & compare
		require.NoError(t, helpers.WaitStoragesSynced(t, Source, Target, 50, helpers.NewCompareStorageParams()))
	})

	t.Run("ALTER ENUM ADD VALUE", func(t *testing.T) {
		_, err := conn.Exec(context.Background(), `ALTER TYPE "fancyEnum" ADD VALUE 'val3';`)
		require.NoError(t, err)
		require.NoError(t, conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
			rows, err := tx.Query(context.Background(), `INSERT INTO __test (id, "Val1", val2, "FancyEnum") VALUES (10, 10, 'f', 'val3')`)
			require.NoError(t, err)
			rows.Close()

			return nil
		}))

		//------------------------------------------------------------------------------------
		// wait & compare

		require.NoError(t, helpers.WaitStoragesSynced(t, Source, Target, 50, helpers.NewCompareStorageParams()))
	})

	t.Run("ADD ENUM ALTER TABLE", func(t *testing.T) {
		_, err := conn.Exec(context.Background(), `create type "superDopeEnum" as enum ('dope', 'dod');`)
		require.NoError(t, err)
		_, err = conn.Exec(context.Background(), `ALTER TABLE __test ADD COLUMN "Dope" "superDopeEnum";`)
		require.NoError(t, err)
		require.NoError(t, conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
			rows, err := tx.Query(context.Background(), `INSERT INTO __test (id, "Val1", val2, "FancyEnum", "Dope") VALUES (12, 10, 'f', 'val3', 'dope')`)
			require.NoError(t, err)
			rows.Close()

			return nil
		}))

		//------------------------------------------------------------------------------------
		// wait & compare

		require.NoError(t, helpers.WaitStoragesSynced(t, Source, Target, 50, helpers.NewCompareStorageParams()))
	})

	require.NoError(t, terminateErr)
}
