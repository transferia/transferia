package usertypes

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	pg_provider "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
)

func init() {
	_ = os.Setenv("YC", "1")                                                                            // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func loadSnapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

	_ = helpers.Activate(t, transfer)

	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}

func checkReplicationWorks(t *testing.T) {
	transfer := model.Transfer{
		ID:   "test_id",
		Src:  &Source,
		Dst:  &Target,
		Type: abstract.TransferTypeSnapshotAndIncrement,
	}

	srcConn, err := pg_provider.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	worker := local.NewLocalWorker(coordinator.NewFakeClient(), &transfer, helpers.EmptyRegistry(), logger.Log)
	worker.Start()
	defer worker.Stop()

	_, err = srcConn.Exec(context.Background(), `INSERT INTO "testschema".test VALUES (2, 'choovuck', 'Value2')`)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitStoragesSynced(t, Source, Target, 50, helpers.NewCompareStorageParams()))

	tag, err := srcConn.Exec(context.Background(), `UPDATE "testschema".test SET deuch = 'Value2' where id = 1`)
	require.NoError(t, err)
	time.Sleep(2 * time.Minute)
	require.EqualValues(t, tag.RowsAffected(), 1)
	require.NoError(t, helpers.WaitStoragesSynced(t, Source, Target, 50, helpers.NewCompareStorageParams()))
}

func TestUserTypesWithSearchPath(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
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
