package replacefkeycommon

import (
	"context"
	"database/sql"
	"errors"
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
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	Source = *helpers.RecipeMysqlSource()
	Target = *helpers.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
)

func init() {
	Source.WithDefaults()
	Target.WithDefaults()
}

func Existence(t *testing.T) {
	_, err := mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = mysql.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)
	require.NoError(t, tasks.ActivateDelivery(context.TODO(), nil, coordinator.NewFakeClient(), *transfer, helpers.EmptyRegistry()))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}

func defaultChecksumParams() *tasks.ChecksumParameters {
	return &tasks.ChecksumParameters{
		TableSizeThreshold: 0,
		Tables: []abstract.TableDescription{
			{Name: "test_src", Schema: Target.Database, Filter: "", EtaRow: uint64(0), Offset: uint64(0)},
			{Name: "test_dst", Schema: Target.Database, Filter: "", EtaRow: uint64(0), Offset: uint64(0)},
		},
		PriorityComparators: nil,
	}
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

	connector := &model.Transfer{
		ID:  "test-id",
		Src: &Source,
		Dst: &Target,
	}

	fakeClient := coordinator.NewStatefulFakeClient()
	err = mysql.SyncBinlogPosition(&Source, connector.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, connector, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotAndIncrement)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("Tables on source: %v", tables)

	cfg := mysql_client.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", Source.Host, Source.Port)
	cfg.User = Source.User
	cfg.Passwd = string(Source.Password)
	cfg.DBName = Source.Database
	cfg.Net = "tcp"

	mysqlConnector, err := mysql_client.NewConnector(cfg)
	require.NoError(t, err)
	db := sql.OpenDB(mysqlConnector)

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	var rollbacks util.Rollbacks
	defer rollbacks.Do()
	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	require.NoError(t, err)
	rollbacks.Add(func() {
		_ = tx.Rollback()
	})
	_, err = tx.Exec("INSERT `test_src` VALUES (1, 'test2') ON DUPLICATE KEY UPDATE `name` = 'test2'")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	rollbacks.Cancel()

	require.NoError(t, waitForSync(t))

	require.NoError(t, tasks.Checksum(*transfer, logger.Log, helpers.EmptyRegistry(), defaultChecksumParams()))
}

func waitForSync(t *testing.T) error {
	cfg := mysql_client.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", Target.Host, Target.Port)
	cfg.User = Target.User
	cfg.Passwd = string(Target.Password)
	cfg.DBName = Target.Database
	cfg.Net = "tcp"

	mysqlConnector, err := mysql_client.NewConnector(cfg)
	require.NoError(t, err)
	db := sql.OpenDB(mysqlConnector)

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	const wait = 1 * time.Second
	const maxWait = 120 * wait // 2 min
	for passed := 0 * time.Second; passed < maxWait; passed += wait {
		time.Sleep(wait)

		var name string
		err := conn.QueryRowContext(context.Background(), "SELECT name FROM test_src WHERE id = 1").Scan(&name)
		if err == nil && name == "test2" {
			return nil
		}
	}

	return errors.New("incorrect rows count or sync timeout")
}
