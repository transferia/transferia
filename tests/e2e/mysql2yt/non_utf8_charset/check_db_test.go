package nonutf8charset

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	mysql_driver2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/transfer"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

const tableName = "kek"

var (
	sourceDatabase = os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE")
	source         = *helpers.WithMysqlInclude(helpers.RecipeMysqlSource(), []string{tableName})
	targetCluster  = os.Getenv("YT_PROXY")
)

func init() {
	source.WithDefaults()
}

func makeConnConfig() *mysql_driver2.Config {
	cfg := mysql_driver2.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", source.Host, source.Port)
	cfg.User = source.User
	cfg.Passwd = string(source.Password)
	cfg.DBName = source.Database
	cfg.Net = "tcp"
	return cfg
}

func makeTarget() provider_yt.YtDestinationModel {
	target := provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:          "//home/cdc/test/mysql2yt/on_utf8_charset",
		Cluster:       targetCluster,
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
	target.WithDefaults()
	return target
}

func TestNonUtf8Charset(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(targetCluster)
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

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt/on_utf8_charset"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	ytDestination := makeTarget()
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &source, ytDestination, abstract.TransferTypeSnapshotAndIncrement)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), &model.TransferOperation{}, transfer, helpers.EmptyRegistry())
	err = snapshotLoader.LoadSnapshot(context.Background())
	require.NoError(t, err)

	require.NoError(t, helpers.CompareStorages(t, source, ytDestination.LegacyModel(), helpers.NewCompareStorageParams()))

	fakeClient := coordinator.NewStatefulFakeClient()
	err = provider_mysql.SyncBinlogPosition(&source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	conn, err := mysql_driver2.NewConnector(makeConnConfig())
	require.NoError(t, err)
	db := sql.OpenDB(conn)
	_, err = db.Exec("INSERT INTO kek VALUES (3, 'Обожаю запах',' напалма', ' по утрам!')")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO kek VALUES (4, 'Где карта,', ' Билли? Нам', ' нужна карта!')")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, sourceDatabase, tableName, helpers.GetSampleableStorageByModel(t, source), helpers.GetSampleableStorageByModel(t, ytDestination.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, source, ytDestination.LegacyModel(), helpers.NewCompareStorageParams()))
}
