package pkeyupdate

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"), pgrecipe.WithDBTables("public.__test"))
	Target       = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
)

func init() {
	_ = os.Setenv("YC", "1")                                                               // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestPkeyUpdate(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	// insert data
	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()
	_, err = srcConn.Exec(context.Background(), `UPDATE __test SET id = 2 WHERE id = 1;`)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), `INSERT INTO __test VALUES (3, 'c');`)
	require.NoError(t, err)

	// wait
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))

	// check
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
}
