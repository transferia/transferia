package usertypes

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func loadSnapshot(t *testing.T) {
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

	_, err = srcConn.Exec(context.Background(), `INSERT INTO "testschema".test VALUES (2, 'choovuck', 'Value2')`)
	require.NoError(t, err)

	require.NoError(t, storagecomparison.WaitStoragesSynced(t, Source, Target, 50, storagecomparison.NewCompareStorageParams()))

	tag, err := srcConn.Exec(context.Background(), `UPDATE "testschema".test SET deuch = 'Value2' where id = 1`)
	require.NoError(t, err)
	time.Sleep(2 * time.Minute)
	require.EqualValues(t, tag.RowsAffected(), 1)
	require.NoError(t, storagecomparison.WaitStoragesSynced(t, Source, Target, 50, storagecomparison.NewCompareStorageParams()))
}

func TestUserTypesWithSearchPath(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	Source.PreSteps.Table = false
	Source.PreSteps.SequenceOwnedBy = false
	Source.PreSteps.Constraint = false
	Source.PreSteps.Collation = false
	Source.PreSteps.Default = false
	Source.PreSteps.MaterializedView = false
	Source.PreSteps.SequenceSet = util.FalsePtr()
	Source.PreSteps.TableAttach = false
	Source.PreSteps.IndexAttach = false

	Source.PostSteps.Table = false
	Source.PostSteps.SequenceOwnedBy = false
	Source.PostSteps.Constraint = false
	Source.PostSteps.Collation = false
	Source.PostSteps.Default = false
	Source.PostSteps.MaterializedView = false
	Source.PostSteps.SequenceSet = util.FalsePtr()
	Source.PostSteps.TableAttach = false
	Source.PostSteps.IndexAttach = false

	Target.Cleanup = model.DisabledCleanup
	loadSnapshot(t)
	// loadSnapshot always assigns true to CopyUpload flag which is used by sinker.
	// In order for replication to work we must set CopyUpload value back to false.
	Target.CopyUpload = false
	checkReplicationWorks(t)
}
