package mysqltoytupdateminimal

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
	"github.com/transferia/transferia/tests/helpers/mysql"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/mysql"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	"github.com/transferia/transferia/tests/helpers/transfer"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

const tableName = "customers"

var (
	source        = *mysql.WithMysqlInclude(mysql.RecipeMysqlSource(), []string{tableName})
	targetCluster = os.Getenv("YT_PROXY")
)

func init() {
	source.WithDefaults()
	source.AllowDecimalAsFloat = true
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
		Path:          "//home/cdc/test/mysql2yt/update_minimal",
		CellBundle:    "default",
		PrimaryMedium: "default",
		Cluster:       targetCluster,
	})
	target.WithDefaults()
	return target
}

func TestUpdateMinimal(t *testing.T) {
	targetPort, err := network.GetPortFromStr(targetCluster)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mysql source", Port: source.Port},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt/update_minimal"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	ytDestination := makeTarget()
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &source, ytDestination, abstract.TransferTypeSnapshotAndIncrement)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), &model.TransferOperation{}, transfer, testmetrics.EmptyRegistry())
	err = snapshotLoader.LoadSnapshot(context.Background())
	require.NoError(t, err)

	require.NoError(t, storagecomparison.CompareStorages(t, source, ytDestination.LegacyModel(), storagecomparison.NewCompareStorageParams()))

	fakeClient := coordinator.NewStatefulFakeClient()
	err = provider_mysql.SyncBinlogPosition(&source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, testmetrics.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	conn, err := mysql_driver2.NewConnector(makeConnConfig())
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

	require.NoError(t, storage.WaitEqualRowsCount(t, source.Database, "customers", storagecomparison.GetSampleableStorageByModel(t, source), storagecomparison.GetSampleableStorageByModel(t, ytDestination.LegacyModel()), 60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, source, ytDestination.LegacyModel(), storagecomparison.NewCompareStorageParams()))
}
