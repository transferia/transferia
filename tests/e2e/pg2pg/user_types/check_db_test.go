package usertypes

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = *pgrecipe.RecipeTarget()
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func loadSnapshot(t *testing.T) {
	Source.PreSteps.Constraint = true
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	_ = delivery.Activate(t, transfer)

	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
}

func checkReplicationWorks(t *testing.T) {
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), `INSERT INTO testtable VALUES (2, 'choovuck', 'zhepa', 'EinScheissdreckWerdeIchTun', (2, '456')::udt, ARRAY [(3, 'foo1')::udt, (4, 'bar1')::udt])`)
	require.NoError(t, err)
	require.NoError(t, storagecomparison.WaitStoragesSynced(t, Source, Target, 50, storagecomparison.NewCompareStorageParams()))

	tag, err := srcConn.Exec(context.Background(), `UPDATE testtable SET fancy = 'zhopa', deuch = 'DuGehstMirAufDieEier', udt = (3, '789')::udt, udt_arr = ARRAY [(5, 'foo2')::udt, (6, 'bar2')::udt] where id = 2`)
	require.NoError(t, err)
	require.EqualValues(t, tag.RowsAffected(), 1)
	require.NoError(t, storagecomparison.WaitStoragesSynced(t, Source, Target, 50, storagecomparison.NewCompareStorageParams()))
}

func TestUserTypes(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	loadSnapshot(t)
	// loadSnapshot always assigns true to CopyUpload flag which is used by sinker.
	// In order for replication to work we must set CopyUpload value back to false.
	Target.CopyUpload = false
	checkReplicationWorks(t)
}
