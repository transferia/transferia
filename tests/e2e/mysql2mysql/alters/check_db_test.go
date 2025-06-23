package alters

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	mysql_client "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = mysql.MysqlSource{
		Host:     os.Getenv("RECIPE_MYSQL_HOST"),
		User:     os.Getenv("RECIPE_MYSQL_USER"),
		Password: model.SecretString(os.Getenv("RECIPE_MYSQL_PASSWORD")),
		Database: os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE"),
		Port:     helpers.GetIntFromEnv("RECIPE_MYSQL_PORT"),
	}
	Target = mysql.MysqlDestination{
		Host:          os.Getenv("TARGET_RECIPE_MYSQL_HOST"),
		User:          os.Getenv("TARGET_RECIPE_MYSQL_USER"),
		Password:      model.SecretString(os.Getenv("TARGET_RECIPE_MYSQL_PASSWORD")),
		Database:      os.Getenv("TARGET_RECIPE_MYSQL_TARGET_DATABASE"),
		Port:          helpers.GetIntFromEnv("TARGET_RECIPE_MYSQL_PORT"),
		SkipKeyChecks: false,
	}
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
			helpers.LabeledPort{Label: "Mysql target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Snapshot", Snapshot)
		t.Run("Replication", Load)
	})
}

func Existence(t *testing.T) {
	_, err := mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = mysql.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.TODO(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
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

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	fakeClient := coordinator.NewStatefulFakeClient()
	err = mysql.SyncBinlogPosition(&Source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("Tables on source: %v", tables)

	sourceCfg := mysql_client.NewConfig()
	sourceCfg.Addr = fmt.Sprintf("%v:%v", Source.Host, Source.Port)
	sourceCfg.User = Source.User
	sourceCfg.Passwd = string(Source.Password)
	sourceCfg.DBName = Source.Database
	sourceCfg.Net = "tcp"

	sourceMysqlConnector, err := mysql_client.NewConnector(sourceCfg)
	require.NoError(t, err)
	sourceDB := sql.OpenDB(sourceMysqlConnector)

	sourceConn, err := sourceDB.Conn(context.Background())
	require.NoError(t, err)

	alterRequestA := "ALTER TABLE `__test_A` ADD `a_current_time` TIMESTAMP;"
	_, err = sourceConn.ExecContext(context.Background(), alterRequestA)
	require.NoError(t, err)

	alterRequestB := "ALTER TABLE `__test_B` DROP COLUMN `b_address`;"
	_, err = sourceConn.ExecContext(context.Background(), alterRequestB)
	require.NoError(t, err)

	alterRequestC := "ALTER TABLE `__test_C` DROP COLUMN `c_uid`;"
	_, err = sourceConn.ExecContext(context.Background(), alterRequestC)
	require.NoError(t, err)

	alterRequestExtensionD := "ALTER TABLE `__test_D` MODIFY `d_id` bigint NOT NULL;"
	_, err = sourceConn.ExecContext(context.Background(), alterRequestExtensionD)
	require.NoError(t, err)

	alterRequestNarrowingD := "ALTER TABLE `__test_D` MODIFY `d_uid` int;"
	_, err = sourceConn.ExecContext(context.Background(), alterRequestNarrowingD)
	require.NoError(t, err)

	var checkTypeD string
	requestCheckTypeD := "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '__test_D' AND COLUMN_NAME = 'd_uid'"
	err = sourceConn.QueryRowContext(context.Background(), requestCheckTypeD).Scan(&checkTypeD)
	require.NoError(t, err)
	require.Equal(t, "int", checkTypeD)

	requestCorrectD := "INSERT INTO `__test_D` (`d_id`, `d_uid`, `d_name`) VALUES (2147483648, 0, 'Joseph');"
	_, err = sourceConn.ExecContext(context.Background(), requestCorrectD)
	require.NoError(t, err)

	// Enables strict SQL mode and an out of range error occurs while inserting bigger or smaller value than supported
	changeOverflowBehaviour := "SET SESSION sql_mode = 'TRADITIONAL';"
	_, err = sourceConn.ExecContext(context.Background(), changeOverflowBehaviour)
	require.NoError(t, err)

	requestIncorrectD := "INSERT INTO `__test_D` (`d_id`, `d_uid`, `d_name`) VALUES (1337, 2147483648, 'Alex');"
	_, err = sourceConn.ExecContext(context.Background(), requestIncorrectD)
	require.Error(t, err)

	err = sourceConn.Close()
	require.NoError(t, err)

	time.Sleep(10 * time.Second)
	// timmyb32r: somewhy test fails if we change it to waiting-polling

	// ---------------------------------------------------------------------

	targetCfg := mysql_client.NewConfig()
	targetCfg.Addr = fmt.Sprintf("%v:%v", Target.Host, Target.Port)
	targetCfg.User = Target.User
	targetCfg.Passwd = string(Target.Password)
	targetCfg.DBName = Target.Database
	targetCfg.Net = "tcp"

	targetMysqlConnector, err := mysql_client.NewConnector(targetCfg)
	require.NoError(t, err)
	targetDB := sql.OpenDB(targetMysqlConnector)

	targetConn, err := targetDB.Conn(context.Background())
	require.NoError(t, err)

	countA := 0
	requestA := "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = database() and TABLE_NAME = '__test_A' and COLUMN_NAME = 'a_current_time';"
	err = targetConn.QueryRowContext(context.Background(), requestA).Scan(&countA)
	require.NoError(t, err)
	require.Equal(t, 1, countA)

	countB := 0
	requestB := "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = database() and TABLE_NAME = '__test_B' and COLUMN_NAME = 'b_address';"
	err = targetConn.QueryRowContext(context.Background(), requestB).Scan(&countB)
	require.NoError(t, err)
	require.Equal(t, 0, countB)

	countC := 0
	requestC := "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = database() and TABLE_NAME = '__test_C' and COLUMN_NAME = 'c_uid';"
	err = targetConn.QueryRowContext(context.Background(), requestC).Scan(&countC)
	require.NoError(t, err)
	require.Equal(t, 0, countC)

	var resultExtensionD int
	requestExtensionD := "SELECT COUNT(*) FROM `__test_D` WHERE `d_id` = 2147483648;"
	err = targetConn.QueryRowContext(context.Background(), requestExtensionD).Scan(&resultExtensionD)
	require.NoError(t, err)
	require.Equal(t, 1, resultExtensionD)

	var resultNarrowingD int
	requestNarrowingD := "SELECT COUNT(*) FROM `__test_D` WHERE `d_id` = 1337;"
	err = targetConn.QueryRowContext(context.Background(), requestNarrowingD).Scan(&resultNarrowingD)
	require.NoError(t, err)
	require.Equal(t, 0, resultNarrowingD)

	err = targetConn.Close()
	require.NoError(t, err)
}
