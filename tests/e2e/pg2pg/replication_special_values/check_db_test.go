package replicationview

import (
	"context"
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

func TestReplicationNullInJSON(t *testing.T) {
	Source := pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target := pgrecipe.RecipeTarget()
	transferType := abstract.TransferTypeSnapshotAndIncrement

	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, Target, transferType)

	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, Target, transferType)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(Source, logger.Log)
	require.NoError(t, err)

	_, err = srcConn.Exec(context.Background(), `INSERT INTO rsv_null_in_json(i, j, jb) VALUES (101, 'null', 'null'), (102, '"null"', '"null"')`)
	require.NoError(t, err)

	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "rsv_null_in_json", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
}
