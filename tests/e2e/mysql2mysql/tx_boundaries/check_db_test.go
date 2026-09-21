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
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/mysql"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/mysql"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	"github.com/transferia/transferia/tests/helpers/transfer"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *mysql.RecipeMysqlSource()
	Target       = *mysql.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
)

func init() {
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	Source.BufferLimit = 100 * 1024                                                        // 100kb to init flush between TX
	Target.PerTransactionPush = true
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mysql source", Port: Source.Port},
			network.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	t.Run("Existence", Existence)
	t.Run("Snapshot", Snapshot)
	t.Run("Replication", Load)
}

func Existence(t *testing.T) {
	_, err := provider_mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = provider_mysql.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))

	targetCfg := mysql_driver2.NewConfig()
	targetCfg.Addr = fmt.Sprintf("%v:%v", Target.Host, Target.Port)
	targetCfg.User = Target.User
	targetCfg.Passwd = string(Target.Password)
	targetCfg.DBName = Target.Database
	targetCfg.Net = "tcp"

	targetMysqlConnector, err := mysql_driver2.NewConnector(targetCfg)
	require.NoError(t, err)
	targetDB := sql.OpenDB(targetMysqlConnector)

	tracker, err := provider_mysql.NewTableProgressTracker(targetDB, Target.Database)
	require.NoError(t, err)
	state, err := tracker.GetCurrentState()
	require.NoError(t, err)
	logger.Log.Info("replication progress", log.Any("progress", state))
	require.Equal(t, 1, len(state))
	require.Equal(t, provider_mysql.SyncWait, state[`"source"."products"`].Status)
	require.True(t, state[`"source"."products"`].LSN > 0)
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

	srcCfg := mysql_driver2.NewConfig()
	srcCfg.Addr = fmt.Sprintf("%v:%v", Source.Host, Source.Port)
	srcCfg.User = Source.User
	srcCfg.Passwd = string(Source.Password)
	srcCfg.DBName = Source.Database
	srcCfg.Net = "tcp"

	srcMysqlConnector, err := mysql_driver2.NewConnector(srcCfg)
	require.NoError(t, err)
	srcDB := sql.OpenDB(srcMysqlConnector)

	srcConn, err := srcDB.Conn(context.Background())
	require.NoError(t, err)

	requests := []string{
		"delete from products where id > 10",
	}

	for _, request := range requests {
		rows, err := srcConn.QueryContext(context.Background(), request)
		require.NoError(t, err)
		require.NoError(t, rows.Close())
	}

	err = srcConn.Close()
	require.NoError(t, err)

	require.NoError(t, storage.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, "products",
		storagecomparison.GetSampleableStorageByModel(t, Source),
		storagecomparison.GetSampleableStorageByModel(t, Target),
		time.Minute))
	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target, storagecomparison.NewCompareStorageParams()))

	targetCfg := mysql_driver2.NewConfig()
	targetCfg.Addr = fmt.Sprintf("%v:%v", Target.Host, Target.Port)
	targetCfg.User = Target.User
	targetCfg.Passwd = string(Target.Password)
	targetCfg.DBName = Target.Database
	targetCfg.Net = "tcp"

	targetMysqlConnector, err := mysql_driver2.NewConnector(targetCfg)
	require.NoError(t, err)
	targetDB := sql.OpenDB(targetMysqlConnector)

	tracker, err := provider_mysql.NewTableProgressTracker(targetDB, Target.Database)
	require.NoError(t, err)
	state, err := tracker.GetCurrentState()
	require.NoError(t, err)
	logger.Log.Info("replication progress", log.Any("progress", state))
	require.Equal(t, 1, len(state))
	require.Equal(t, provider_mysql.InSync, state[`"source"."products"`].Status)
	require.True(t, state[`"source"."products"`].LSN > 0)
}
