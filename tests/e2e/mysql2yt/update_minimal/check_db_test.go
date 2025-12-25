package mysqltoytupdateminimal

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	mysql_source "github.com/transferia/transferia/pkg/providers/mysql"
	ytcommon "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

const tableName = "customers"

var (
	source        = *helpers.WithMysqlInclude(helpers.RecipeMysqlSource(), []string{tableName})
	targetCluster = os.Getenv("YT_PROXY")
)

func init() {
	source.WithDefaults()
	source.AllowDecimalAsFloat = true
}

func makeConnConfig() *mysql.Config {
	cfg := mysql.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", source.Host, source.Port)
	cfg.User = source.User
	cfg.Passwd = string(source.Password)
	cfg.DBName = source.Database
	cfg.Net = "tcp"
	return cfg
}

func makeTarget() ytcommon.YtDestinationModel {
	target := ytcommon.NewYtDestinationV1(ytcommon.YtDestination{
		Path:          "//home/cdc/test/mysql2yt/update_minimal",
		CellBundle:    "default",
		PrimaryMedium: "default",
		Cluster:       targetCluster,
	})
	target.WithDefaults()
	return target
}

func TestUpdateMinimal(t *testing.T) {
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

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt/update_minimal"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	ytDestination := makeTarget()
	transfer := helpers.MakeTransfer(helpers.TransferID, &source, ytDestination, abstract.TransferTypeSnapshotAndIncrement)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.LoadSnapshot(context.Background())
	require.NoError(t, err)

	require.NoError(t, helpers.CompareStorages(t, source, ytDestination.LegacyModel(), helpers.NewCompareStorageParams()))

	fakeClient := coordinator.NewStatefulFakeClient()
	err = mysql_source.SyncBinlogPosition(&source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	conn, err := mysql.NewConnector(makeConnConfig())
	require.NoError(t, err)

	requests := []string{
		"set session sql_mode=''",
		"update customers set status = 'active,waiting' where customerNumber in (131, 141);",
		"update customers set status = '' where customerNumber in (103, 141);",
		"update customers set contactLastName = '', contactFirstName = NULL where customerNumber in (129, 131, 141);",
		"update customers set contactLastName = 'Lollers', contactFirstName = 'Kekus' where customerNumber in (103, 112, 114, 119);",
		"update customers set customerName = 'Kabanchik INC', city = 'Los Hogas' where customerNumber in (121, 124, 125, 128);",
		"update customers set customerSize = 'medium' where customerNumber in (112, 114);",
		"update customers set customerSize = 'big' where customerNumber in (128);",
		"update customers set customerSize = '' where customerNumber in (103);",
	}

	db := sql.OpenDB(conn)
	for _, request := range requests {
		_, err := db.Exec(request)
		require.NoError(t, err)
	}

	_, err = db.Exec("delete from customers where customerNumber = 114")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, source.Database, "customers", helpers.GetSampleableStorageByModel(t, source), helpers.GetSampleableStorageByModel(t, ytDestination.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, source, ytDestination.LegacyModel(), helpers.NewCompareStorageParams()))
}
