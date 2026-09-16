package mysqltoytcollapse

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
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers/mysql"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

const tableName = "test"

var (
	source        = *mysql.WithMysqlInclude(mysql.RecipeMysqlSource(), []string{tableName})
	targetCluster = os.Getenv("YT_PROXY")
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
		Path:          "//home/cdc/test/mysql2yt/collapse",
		Cluster:       targetCluster,
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
	target.WithDefaults()
	return target
}

func TestCollapse(t *testing.T) {
	targetPort, err := network.GetPortFromStr(targetCluster)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mysql source", Port: source.Port},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ytDestination := makeTarget()
	transfer := *transferhelpers.MakeTransfer("collapse_test", &source, ytDestination, "")

	fakeClient := coordinator.NewStatefulFakeClient()
	err = provider_mysql.SyncBinlogPosition(&source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, &transfer, testmetrics.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	conn, err := mysql_driver2.NewConnector(makeConnConfig())
	require.NoError(t, err)

	requests := []string{
		"insert into test (id, value) values(1, 'aaa');",
		"delete from test where id = 1;",
		"insert into test (id, value) values(1, 'bbb');",
	}

	db := sql.OpenDB(conn)
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)
	for _, request := range requests {
		_, err := tx.Query(request)
		require.NoError(t, err)
	}
	err = tx.Commit()
	require.NoError(t, err)

	require.NoError(t, storage.WaitEqualRowsCount(t, source.Database, tableName, storagecomparison.GetSampleableStorageByModel(t, source), storagecomparison.GetSampleableStorageByModel(t, ytDestination.LegacyModel()), 60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, source, ytDestination.LegacyModel(), storagecomparison.NewCompareStorageParams()))
}
