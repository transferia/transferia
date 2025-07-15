package alters

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	abstract_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/e2e/pg2ch"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotOnly
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
	Target.Cleanup = abstract_model.DisabledCleanup
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
	transfer := helpers.MakeTransferForIncrementalSnapshot(helpers.TransferID, &Source, &Target, TransferType, "public", "__test", "id", "0", 1)
	cp := helpers.NewFakeCP()
	_, err = helpers.ActivateWithCP(transfer, cp)
	require.NoError(t, err)
	require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
	t.Run("ADD COLUMN", func(t *testing.T) {

		rows, err := conn.Query(context.Background(), "INSERT INTO __test (id, val1, val2) VALUES (6, 6, 'c')")
		require.NoError(t, err)
		rows.Close()
		rows, err = conn.Query(context.Background(), "ALTER TABLE __test ADD COLUMN new_val INTEGER")
		require.NoError(t, err)
		rows.Close()

		time.Sleep(10 * time.Second)

		rows, err = conn.Query(context.Background(), "INSERT INTO __test (id, val1, val2, new_val) VALUES (7, 7, 'd', 7)")
		require.NoError(t, err)
		rows.Close()

		t.Log("activating transfer after alter")
		_, err = helpers.ActivateWithCP(transfer, cp)
		require.NoError(t, err)
		t.Log("activation is done")
		require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
		require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams().WithEqualDataTypes(pg2ch.PG2CHDataTypesComparator)))
	})

}
