package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	databaseName = "public"
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump/pg"), pgrecipe.WithPrefix(""))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitDir("dump/ch"), chrecipe.WithDatabase(databaseName))
)

func init() {
	_ = os.Setenv("YC", "1")                                                               // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestSnapshotAndIncrement(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
		))
	}()

	//------------------------------------------------------------------------------------
	// start worker

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)

	err := tasks.ActivateDelivery(context.Background(), nil, coordinator.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------------
	// wait & compare

	require.NoError(t, helpers.WaitEqualRowsCount(t, databaseName, "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 60*time.Second))
}
