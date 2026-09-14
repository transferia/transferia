package byteakey

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
	"github.com/transferia/transferia/tests/helpers"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	TransferType = abstract.TransferTypeIncrementOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"), pgrecipe.WithDBTables("public.test"))
	Target       = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
)

func init() {
	_ = os.Setenv("YC", "1")                                                               // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestByteaKey(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()
	dstConn, err := provider_postgres.MakeConnPoolFromDst(&Target, logger.Log)
	require.NoError(t, err)
	defer dstConn.Close()

	_, err = srcConn.Exec(context.Background(), `INSERT INTO test VALUES ('\xdeadbeef', 'a')`)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), `UPDATE test SET value = 'b'`)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), `INSERT INTO test VALUES ('\xB16B00B5', 'b')`)
	require.NoError(t, err)

	// wait
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))

	// check
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
