package alters

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/e2e/pg2ch"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"), pgrecipe.WithPrefix(""))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestAlter(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	connConfig, err := provider_postgres.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	conn, err := provider_postgres.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	//------------------------------------------------------------------------------------
	// start worker

	Target.ProtocolUnspecified = true
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{"public.__test"}}
	var terminateErr error
	localWorker := delivery.Activate(t, transfer, func(err error) {
		terminateErr = err
	})
	defer localWorker.Close(t)

	t.Run("ADD COLUMN with defaults", func(t *testing.T) {
		// force INSERTs with different schemas to be pushed with one ApplyChangeItems call
		err := conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
			rows, err := tx.Query(context.Background(), "INSERT INTO __test (id, val1, val2) VALUES (3, 3, 'e')")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "ALTER TABLE __test ADD COLUMN new_val1 TEXT DEFAULT 'test default value'")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "ALTER TABLE __test ADD COLUMN new_val2 INTEGER DEFAULT 1")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "INSERT INTO __test (id, val1, val2, new_val1, new_val2) VALUES (4, 4, 'f', '4', 4)")
			require.NoError(t, err)
			rows.Close()
			return nil
		})
		require.NoError(t, err)

		//------------------------------------------------------------------------------------
		// wait & compare

		require.NoError(t, storage.WaitEqualRowsCount(t, databaseName, "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))
		require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
	})

	t.Run("ADD COLUMN with complex defaults", func(t *testing.T) {
		// force INSERTs with different schemas to be pushed with one ApplyChangeItems call
		err := conn.BeginFunc(context.Background(), func(tx pgx.Tx) error {
			rows, err := tx.Query(context.Background(), "INSERT INTO __test (id, val1, val2) VALUES (5, 5, 'e')")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "ALTER TABLE __test ADD COLUMN new_val3 TEXT DEFAULT pg_size_pretty(EXTRACT(EPOCH from  now())::bigint)")
			require.NoError(t, err)
			rows.Close()

			rows, err = tx.Query(context.Background(), "INSERT INTO __test (id, val1, val2, new_val1, new_val2) VALUES (6, 6, 'f', '6', 6)")
			require.NoError(t, err)
			rows.Close()
			return nil
		})
		require.NoError(t, err)

		//------------------------------------------------------------------------------------
		// wait & compare

		st := time.Now()
		for time.Since(st) < time.Minute {
			time.Sleep(time.Second)
			if terminateErr != nil {
				break
			}
		}
		require.Error(t, terminateErr)
		require.True(t, abstract.IsFatal(terminateErr))
	})
}
