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
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""))
	Target = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)
}

func TestSessionReplicationRole(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	Source.PreSteps.Trigger = true
	Source.PostSteps.Trigger = false
	Target.IgnoreTriggersAndRules = true
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))

	_, err = srcConn.Exec(context.Background(), `INSERT INTO __test_trigger (id, value) VALUES (3, 'c')`)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), `UPDATE __test_trigger SET value = 'c_updated' WHERE id = 3`)
	require.NoError(t, err)

	require.NoError(t, storagecomparison.WaitStoragesSynced(t, Source, Target, 50, storagecomparison.NewCompareStorageParams()))
}
