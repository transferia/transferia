package replication

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	source = helpers.RecipeMysqlSource()
	target = yt_helpers.RecipeYtTarget("//home/cdc/test/mysql2yt_e2e_replication")

	sourceDatabase   = os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE")
	tableNotIncluded = ypath.Path(fmt.Sprintf("//home/cdc/test/mysql2yt_e2e_replication/%s___not_included_test", sourceDatabase))
)

func init() {
	source.WithDefaults()
}

func makeConnConfig() *mysqlDriver.Config {
	cfg := mysqlDriver.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", source.Host, source.Port)
	cfg.User = source.User
	cfg.Passwd = string(source.Password)
	cfg.DBName = source.Database
	cfg.Net = "tcp"
	return cfg
}

func TestGroup(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_replication"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_replication"), &yt.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	t.Run("Load", Load)
}

func Load(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{fmt.Sprintf("%s.__test", sourceDatabase)}}

	ctx := context.Background()

	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err := snapshotLoader.LoadSnapshot(ctx)
	require.NoError(t, err)

	fakeClient := coordinator.NewStatefulFakeClient()
	err = mysql.SyncBinlogPosition(source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	conn, err := mysqlDriver.NewConnector(makeConnConfig())
	require.NoError(t, err)
	db := sql.OpenDB(conn)
	_, err = db.Exec("INSERT INTO `__test` (`id`, `value`) VALUES (3, 'stereo')")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO `__test` (`id`, `value`) VALUES (4, 'retroCarzzz')")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO `__not_included_test` (`id`, `value`) VALUES (4, 'retroCarzzz')")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, source.Database, "__test", helpers.GetSampleableStorageByModel(t, source), helpers.GetSampleableStorageByModel(t, target.LegacyModel()), 60*time.Second))

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	exists, err := ytEnv.YT.NodeExists(context.Background(), tableNotIncluded, nil)
	require.NoError(t, err)
	require.False(t, exists)
}
