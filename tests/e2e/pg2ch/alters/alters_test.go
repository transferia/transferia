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
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
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

func TestAlter(t *testing.T) {
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

	Target.ProtocolUnspecified = true
	Target.MigrationOptions = &model.ChSinkMigrationOptions{
		AddNewColumns: true,
	}
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	var terminateErr error
	localWorker := helpers.Activate(t, transfer, func(err error) {
		terminateErr = err
	})
	defer localWorker.Close(t)

	t.Run("ADD COLUMN", func(t *testing.T) {
		rows, err := conn.Query(context.Background(), "INSERT INTO __test (id, val1, val2) VALUES (6, 6, 'c')")
		require.NoError(t, err)
		rows.Close()

		require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))

		rows, err = conn.Query(context.Background(), "ALTER TABLE __test ADD COLUMN new_val INTEGER")
		require.NoError(t, err)
		rows.Close()

		time.Sleep(10 * time.Second)

		rows, err = conn.Query(context.Background(), "INSERT INTO __test (id, val1, val2, new_val) VALUES (7, 7, 'd', 7)")
		require.NoError(t, err)
		rows.Close()

		//------------------------------------------------------------------------------------
		// wait & compare

		require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
		require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
	})

	t.Run("ADD COLUMN single transaction", func(t *testing.T) {
		// force INSERTs with different schemas to be pushed with one ApplyChangeItems call
		err := conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
			rows, err := tx.Query(context.Background(), "INSERT INTO __test (id, val1, val2) VALUES (8, 8, 'e')")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "ALTER TABLE __test ADD COLUMN new_val2 INTEGER")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "INSERT INTO __test (id, val1, val2, new_val2) VALUES (9, 9, 'f', 9)")
			require.NoError(t, err)
			rows.Close()
			return nil
		})
		require.NoError(t, err)

		//------------------------------------------------------------------------------------
		// wait & compare

		require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
		require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
	})

	// Add temporary column, shall terminate replication
	t.Run("ADD TEMPORARY COLUMN", func(t *testing.T) {
		// add column, with one new change
		require.NoError(t, conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
			rows, err := tx.Query(context.Background(), "INSERT INTO __test (id, val1, val2, new_val2) VALUES (10, 10, 'f', 10)")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "ALTER TABLE __test ADD COLUMN new_val3 INTEGER")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "INSERT INTO __test (id, val1, val2, new_val2, new_val3) VALUES (11, 11, 'f', 11, 11)")
			require.NoError(t, err)
			rows.Close()

			return nil
		}))

		// delete new column, with one new change without this column
		require.NoError(t, conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
			rows, err := tx.Query(context.Background(), "ALTER TABLE __test DROP COLUMN new_val3")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "INSERT INTO __test (id, val1, val2, new_val2) VALUES (12, 12, 'f', 12)")
			require.NoError(t, err)
			rows.Close()

			return nil
		}))

		//------------------------------------------------------------------------------------
		// wait termination

		require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
		require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
	})

	require.NoError(t, terminateErr)
}
