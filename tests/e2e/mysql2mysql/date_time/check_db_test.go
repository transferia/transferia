package datetime

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	mysql_driver2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/mysql"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	"github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *mysql.RecipeMysqlSource()
	Target       = *mysql.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
)

func init() {
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mysql source", Port: Source.Port},
			network.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	t.Run("Main group", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Snapshot", Snapshot)
		t.Run("Replication", Load)
	})
}

func Existence(t *testing.T) {
	_, err := provider_mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = provider_mysql.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)
	tables, err := tasks.ObtainAllSrcTables(transfer, testmetrics.EmptyRegistry())
	require.NoError(t, err)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), &model.TransferOperation{}, transfer, testmetrics.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.TODO(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
}

func Load(t *testing.T) {
	sourceAsDestination := provider_mysql.MysqlDestination{
		Host:     Source.Host,
		User:     Source.User,
		Password: Source.Password,
		Database: Source.Database,
		Port:     Source.Port,
	}
	sourceAsDestination.WithDefaults()
	_, err := provider_mysql.NewSinker(logger.Log, &sourceAsDestination, testmetrics.EmptyRegistry())
	require.NoError(t, err)

	transfer := &model.Transfer{
		ID:  "test-id",
		Src: &Source,
		Dst: &Target,
	}

	fakeClient := coordinator.NewStatefulFakeClient()
	err = provider_mysql.SyncBinlogPosition(&Source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, testmetrics.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	cfg := mysql_driver2.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", Source.Host, Source.Port)
	cfg.User = Source.User
	cfg.Passwd = string(Source.Password)
	cfg.DBName = Source.Database
	cfg.Net = "tcp"

	mysqlConnector, err := mysql_driver2.NewConnector(cfg)
	require.NoError(t, err)
	db := sql.OpenDB(mysqlConnector)

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	modeRequest := `SET SESSION sql_mode=''`          // drop strict mode
	timeRequest := `SET SESSION time_zone = '+00:00'` // set UTC to check corner cases
	insertRequest1 := `INSERT INTO __test1 (col_d, col_dt, col_ts) VALUES
		('0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00'),
		('1000-01-01', '1000-01-01 00:00:00', '1970-01-01 00:00:01'),
		('9999-12-31', '9999-12-31 23:59:59', '2038-01-19 03:14:07'),
		('2020-12-23', '2020-12-23 14:15:16', '2020-12-23 14:15:16')`
	insertRequest2 := `INSERT INTO __test2 (col_dt1, col_dt2, col_dt3, col_dt4, col_dt5, col_dt6, col_ts1, col_ts2, col_ts3, col_ts4, col_ts5, col_ts6) VALUES
		('2020-12-23 14:15:16.1', '2020-12-23 14:15:16.12', '2020-12-23 14:15:16.123', '2020-12-23 14:15:16.1234', '2020-12-23 14:15:16.12345', '2020-12-23 14:15:16.123456','2020-12-23 14:15:16.1', '2020-12-23 14:15:16.12', '2020-12-23 14:15:16.123', '2020-12-23 14:15:16.1234', '2020-12-23 14:15:16.12345', '2020-12-23 14:15:16.123456'),
		('2020-12-23 14:15:16.6', '2020-12-23 14:15:16.65', '2020-12-23 14:15:16.654', '2020-12-23 14:15:16.6543', '2020-12-23 14:15:16.65432', '2020-12-23 14:15:16.654321','2020-12-23 14:15:16.6', '2020-12-23 14:15:16.65', '2020-12-23 14:15:16.654', '2020-12-23 14:15:16.6543', '2020-12-23 14:15:16.65432', '2020-12-23 14:15:16.654321')`

	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	require.NoError(t, err)

	_, err = tx.Query(modeRequest)
	require.NoError(t, err)
	_, err = tx.Query(timeRequest)
	require.NoError(t, err)
	_, err = tx.Query(insertRequest1)
	require.NoError(t, err)
	_, err = tx.Query(insertRequest2)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	err = conn.Close()
	require.NoError(t, err)

	require.NoError(t, storage.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, "__test1",
		storagecomparison.GetSampleableStorageByModel(t, Source),
		storagecomparison.GetSampleableStorageByModel(t, Target),
		60*time.Second))
	require.NoError(t, storage.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, "__test2",
		storagecomparison.GetSampleableStorageByModel(t, Source),
		storagecomparison.GetSampleableStorageByModel(t, Target),
		60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
}
