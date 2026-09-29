package light

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
	_ "github.com/transferia/transferia/tests/helpers/registration/mysql"
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

	t.Run("Group after port check", func(t *testing.T) {
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
	require.NoError(t, snapshotLoader.UploadTables(context.TODO(), tables.ConvertToTableDescriptions(), true))

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

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)

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

	modeRequest := `SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'`
	insertRequest := `INSERT INTO __test_special_values (id, data) VALUES
		(0, 1),
		(NULL, 2),
		(NULL, 3)`

	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	require.NoError(t, err)

	_, err = tx.Query(modeRequest)
	require.NoError(t, err)
	_, err = tx.Query(insertRequest)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	err = conn.Close()
	require.NoError(t, err)

	require.NoError(t, storage.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, "__test_special_values",
		storagecomparison.GetSampleableStorageByModel(t, Source),
		storagecomparison.GetSampleableStorageByModel(t, Target),
		60*time.Second))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))
}
