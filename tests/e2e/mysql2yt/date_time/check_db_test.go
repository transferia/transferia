package mysqltoytdatetime

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
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

const (
	tableName           = "time_test"
	layoutDateMySQL     = "2006-01-02"
	layoutDatetimeMySQL = "2006-01-02 15:04:05.999999"
)

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

func makeTarget() model.Destination {
	target := provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:          "//home/cdc/test/mysql2yt/date_time",
		Cluster:       targetCluster,
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
	target.WithDefaults()
	return target
}

func ParseDate(value string) ytschema.Date {
	date, _ := time.Parse(layoutDateMySQL, value)
	schemaDate, err := ytschema.NewDate(date)
	if err != nil {
		panic(err)
	}
	return schemaDate
}

func TestDateTime(t *testing.T) {
	targetPort, err := network.GetPortFromStr(targetCluster)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mysql source", Port: source.Port},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	t.Setenv("YC", "1") // to not go to vanga

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt/date_time"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	ytDestination := makeTarget()
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &source, ytDestination, abstract.TransferTypeSnapshotAndIncrement)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), &model.TransferOperation{}, transfer, testmetrics.EmptyRegistry())
	err = snapshotLoader.LoadSnapshot(context.Background())
	require.NoError(t, err)

	require.NoError(t, storagecomparison.CompareStorages(t, source, ytDestination.(provider_yt.YtDestinationModel).LegacyModel(), storagecomparison.NewCompareStorageParams()))

	fakeClient := coordinator.NewStatefulFakeClient()
	err = provider_mysql.SyncBinlogPosition(&source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, testmetrics.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	conn, err := mysql_driver2.NewConnector(makeConnConfig())
	require.NoError(t, err)
	db := sql.OpenDB(conn)
	_, err = db.Exec(`INSERT INTO time_test VALUES (101, '2022-12-25', '2022-12-25 14:15:16', '2022-12-25 14:15:16')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO time_test VALUES (102, '2022-12-26', '2022-12-26 14:15:16', '2022-12-26 14:15:16')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO time_test VALUES (103, '1970-01-01', '1970-01-01 00:00:00', '1970-01-01 00:00:00')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO time_test VALUES (104, NULL, NULL, NULL)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO time_test VALUES (105, '1989-11-09', '1989-11-09 19:02:03.456789', '1989-11-09 19:02:03.456789')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO time_test VALUES (106, '1970-01-01', '1970-01-01 00:00:00', '1970-01-01 00:00:00')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO time_test VALUES (107, '2025-05-25', '2025-05-25 00:05:25.555', '2025-05-25 00:05:25.555555')`)
	require.NoError(t, err)

	require.NoError(t, storage.WaitEqualRowsCount(t, source.Database, tableName, storagecomparison.GetSampleableStorageByModel(t, source), storagecomparison.GetSampleableStorageByModel(t, ytDestination.(provider_yt.YtDestinationModel).LegacyModel()), 60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, source, ytDestination.(provider_yt.YtDestinationModel).LegacyModel(), storagecomparison.NewCompareStorageParams()))
}
