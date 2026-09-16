package light

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/pkg/sink_factory"
	"github.com/transferia/transferia/pkg/worker/tasks"
	cleanup_task "github.com/transferia/transferia/pkg/worker/tasks/cleanup"
	"github.com/transferia/transferia/tests/helpers/mysql"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	Source              = *mysql.RecipeMysqlSource()
	SourceWithBlackList = *mysql.WithMysqlInclude(mysql.RecipeMysqlSource(), []string{"items_.*"})
	Target              = *mysql.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
)

func init() {
	_ = os.Setenv("YC", "1")                                                                                            // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mysql source", Port: Source.Port},
			network.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Drop by filter", TruncateAll)
		t.Run("Drop by filter", DropFilter)
		t.Run("Drop all tables", DropAll)
	})
}

func DropAll(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)

	tables, err := tasks.ObtainAllSrcTables(transfer, testmetrics.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	sink, err := sink_factory.MakeAsyncSink(transfer, &model.TransferOperation{}, logger.Log, testmetrics.EmptyRegistry(), coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	require.NoError(t, err)

	err = cleanup_task.CleanupTables(sink, tables, model.Drop)
	require.NoError(t, err)
}

func DropFilter(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &SourceWithBlackList, &Target, abstract.TransferTypeSnapshotAndIncrement)

	tables, err := tasks.ObtainAllSrcTables(transfer, testmetrics.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	sink, err := sink_factory.MakeAsyncSink(transfer, &model.TransferOperation{}, logger.Log, testmetrics.EmptyRegistry(), coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	require.NoError(t, err)

	err = cleanup_task.CleanupTables(sink, tables, model.Drop)
	require.NoError(t, err)
}

func TruncateAll(t *testing.T) {
	dstCopy := Target
	dstCopy.Cleanup = model.Truncate
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &dstCopy, abstract.TransferTypeSnapshotAndIncrement)

	tables, err := tasks.ObtainAllSrcTables(transfer, testmetrics.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	sink, err := sink_factory.MakeAsyncSink(transfer, &model.TransferOperation{}, logger.Log, testmetrics.EmptyRegistry(), coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	require.NoError(t, err)

	err = cleanup_task.CleanupTables(sink, tables, model.Truncate)
	require.NoError(t, err)
}
