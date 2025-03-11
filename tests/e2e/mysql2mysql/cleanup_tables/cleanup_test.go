package light

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/middlewares"
	"github.com/transferria/transferria/pkg/sink"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/pkg/worker/tasks/cleanup"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	Source              = *helpers.RecipeMysqlSource()
	SourceWithBlackList = *helpers.WithMysqlInclude(helpers.RecipeMysqlSource(), []string{"items_.*"})
	Target              = *helpers.RecipeMysqlTarget()
)

func init() {
	_ = os.Setenv("YC", "1")                                                                            // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Drop by filter", TruncateAll)
		t.Run("Drop by filter", DropFilter)
		t.Run("Drop all tables", DropAll)
	})
}

func DropAll(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	sink, err := sink.MakeAsyncSink(transfer, logger.Log, helpers.EmptyRegistry(), coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	require.NoError(t, err)

	err = cleanup.CleanupTables(sink, tables, model.Drop)
	require.NoError(t, err)
}

func DropFilter(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &SourceWithBlackList, &Target, abstract.TransferTypeSnapshotAndIncrement)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	sink, err := sink.MakeAsyncSink(transfer, logger.Log, helpers.EmptyRegistry(), coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	require.NoError(t, err)

	err = cleanup.CleanupTables(sink, tables, model.Drop)
	require.NoError(t, err)
}

func TruncateAll(t *testing.T) {
	dstCopy := Target
	dstCopy.Cleanup = model.Truncate
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &dstCopy, abstract.TransferTypeSnapshotAndIncrement)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	sink, err := sink.MakeAsyncSink(transfer, logger.Log, helpers.EmptyRegistry(), coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	require.NoError(t, err)

	err = cleanup.CleanupTables(sink, tables, model.Truncate)
	require.NoError(t, err)
}
