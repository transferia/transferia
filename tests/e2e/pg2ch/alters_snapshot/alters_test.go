package alters

import (
	"context"
	"os"
	"testing"
	"time"

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
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	"github.com/transferia/transferia/tests/helpers/yatestx"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir(yatestx.ProjectSource("dump/pg")), pgrecipe.WithPrefix(""))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir(yatestx.ProjectSource("dump/ch")), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("YC", "1")                                                               // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestAlter(t *testing.T) {
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

	Target.ProtocolUnspecified = true
	transfer := transferhelpers.MakeTransferForIncrementalSnapshot(transferhelpers.TransferID, &Source, &Target, TransferType, "public", "__test", "id", "0", 1)
	cp := delivery.NewFakeCPErrRepl()
	_, err = delivery.ActivateWithCP(transfer, cp, true)
	require.NoError(t, err)
	require.NoError(t, storage.WaitEqualRowsCount(t, databaseName, "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
	t.Run("ADD COLUMN", func(t *testing.T) {

		rows, err := conn.Query(context.Background(), "INSERT INTO __test (id, val1, val2) VALUES (6, 6, 'c')")
		require.NoError(t, err)
		rows.Close()
		rows, err = conn.Query(context.Background(), "ALTER TABLE __test ADD COLUMN new_val INTEGER")
		require.NoError(t, err)
		rows.Close()
		rows, err = conn.Query(context.Background(), "ALTER TABLE __test ALTER COLUMN to_alter1 TYPE BIGINT")
		require.NoError(t, err)
		rows.Close()

		time.Sleep(10 * time.Second)

		rows, err = conn.Query(context.Background(), "INSERT INTO __test (id, val1, val2, new_val, to_alter1) VALUES (7, 7, 'd', 7, 7)")
		require.NoError(t, err)
		rows.Close()

		t.Log("activating transfer after alter")
		_, err = delivery.ActivateWithCP(transfer, cp, true)
		require.NoError(t, err)
		t.Log("activation is done")
		require.NoError(t, storage.WaitEqualRowsCount(t, databaseName, "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))
		require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
	})

}
