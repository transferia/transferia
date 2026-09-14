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
	"github.com/transferia/transferia/tests/helpers"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
)

func init() {
	_ = os.Setenv("YC", "1")                                                                                            // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func loadSnapshot(t *testing.T) {
	Source.PreSteps.Constraint = true
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	helpers.Activate(t, transfer)

	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}

func checkReplicationWorks(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	_, err = srcConn.Exec(context.Background(), `INSERT INTO testtable VALUES (5, '{"k5": {"k55": {"val55": 5}}}')`)
	require.NoError(t, err)
	require.NoError(t, helpers.WaitStoragesSynced(t, Source, Target, 50, helpers.NewCompareStorageParams()))
}

func TestUserTypes(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	Target.CopyUpload = false
	loadSnapshot(t)
	checkReplicationWorks(t)
}
