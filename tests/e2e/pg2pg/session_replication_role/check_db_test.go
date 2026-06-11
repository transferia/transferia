package sessionreplicationrole

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
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""))
	Target = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)
}

func TestSessionReplicationRole(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	Source.PreSteps.Trigger = true
	Source.PostSteps.Trigger = false
	Target.IgnoreTriggersAndRules = true
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))

	_, err = srcConn.Exec(context.Background(), `INSERT INTO __test_trigger (id, value) VALUES (3, 'c')`)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), `UPDATE __test_trigger SET value = 'c_updated' WHERE id = 3`)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitStoragesSynced(t, Source, Target, 50, helpers.NewCompareStorageParams()))
}
