package replicationwithoutpk

import (
	"context"
	"fmt"
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

const tableName = "public.__test"

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"))
	Target       = pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestUpdatesWithoutSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, Target, TransferType)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, Target, TransferType)

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(Source, logger.Log)
	require.NoError(t, err)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	_, err = srcConn.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s VALUES (1,6);", tableName))
	require.NoError(t, err)

	_, err = srcConn.Exec(context.Background(), fmt.Sprintf("UPDATE %s SET a=1;", tableName))
	require.NoError(t, err)

	_, err = srcConn.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s VALUES (7,8);", tableName))
	require.NoError(t, err)

	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
}
