package light

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	mysql_client "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *helpers.RecipeMysqlSource()
	Target       = *helpers.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
)

func init() {
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	Source.BufferLimit = 100 * 1024                                        // 100kb to init flush between TX
	Target.PerTransactionPush = true
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	t.Run("Existence", Existence)
	t.Run("Snapshot", Snapshot)
	t.Run("Replication", Load)
}

func Existence(t *testing.T) {
	_, err := mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = mysql.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))

	targetCfg := mysql_client.NewConfig()
	targetCfg.Addr = fmt.Sprintf("%v:%v", Target.Host, Target.Port)
	targetCfg.User = Target.User
	targetCfg.Passwd = string(Target.Password)
	targetCfg.DBName = Target.Database
	targetCfg.Net = "tcp"

	targetMysqlConnector, err := mysql_client.NewConnector(targetCfg)
	require.NoError(t, err)
	targetDB := sql.OpenDB(targetMysqlConnector)

	tracker, err := mysql.NewTableProgressTracker(targetDB, Target.Database)
	require.NoError(t, err)
	state, err := tracker.GetCurrentState()
	require.NoError(t, err)
	logger.Log.Info("replication progress", log.Any("progress", state))
	require.Equal(t, 1, len(state))
	require.Equal(t, mysql.SyncWait, state[`"source"."products"`].Status)
	require.True(t, state[`"source"."products"`].LSN > 0)
}

func Load(t *testing.T) {
	sourceAsDestination := mysql.MysqlDestination{
		Host:     Source.Host,
		User:     Source.User,
		Password: Source.Password,
		Database: Source.Database,
		Port:     Source.Port,
	}
	sourceAsDestination.WithDefaults()
	_, err := mysql.NewSinker(logger.Log, &sourceAsDestination, helpers.EmptyRegistry())
	require.NoError(t, err)

	transfer := &model.Transfer{
		ID:  "test-id",
		Src: &Source,
		Dst: &Target,
	}

	fakeClient := coordinator.NewStatefulFakeClient()
	err = mysql.SyncBinlogPosition(&Source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	srcCfg := mysql_client.NewConfig()
	srcCfg.Addr = fmt.Sprintf("%v:%v", Source.Host, Source.Port)
	srcCfg.User = Source.User
	srcCfg.Passwd = string(Source.Password)
	srcCfg.DBName = Source.Database
	srcCfg.Net = "tcp"

	srcMysqlConnector, err := mysql_client.NewConnector(srcCfg)
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

	require.NoError(t, helpers.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, "products",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target),
		time.Minute))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))

	targetCfg := mysql_client.NewConfig()
	targetCfg.Addr = fmt.Sprintf("%v:%v", Target.Host, Target.Port)
	targetCfg.User = Target.User
	targetCfg.Passwd = string(Target.Password)
	targetCfg.DBName = Target.Database
	targetCfg.Net = "tcp"

	targetMysqlConnector, err := mysql_client.NewConnector(targetCfg)
	require.NoError(t, err)
	targetDB := sql.OpenDB(targetMysqlConnector)

	tracker, err := mysql.NewTableProgressTracker(targetDB, Target.Database)
	require.NoError(t, err)
	state, err := tracker.GetCurrentState()
	require.NoError(t, err)
	logger.Log.Info("replication progress", log.Any("progress", state))
	require.Equal(t, 1, len(state))
	require.Equal(t, mysql.InSync, state[`"source"."products"`].Status)
	require.True(t, state[`"source"."products"`].LSN > 0)
}
