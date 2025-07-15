package light

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	mysql_client "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotAndIncrement
	Source       = *helpers.RecipeMysqlSource()
	Target       = *helpers.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
)

func init() {
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
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)

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

	insertRequest := strings.ReplaceAll(`
			INSERT INTO __test
			(%stinyint%s, %stinyint_def%s, %stinyint_u%s, %stinyint_z%s, %ssmallint%s, %ssmallint_u%s, %ssmallint_z%s, %smediumint%s, %smediumint_u%s, %smediumint_z%s, %sint%s       , %sint_u%s    , %sint_z%s    , %sbigint%s             , %sbigint_u%s           , %sbigint_z%s           ,  %sbool%s, %sdecimal_10_2%s ,%sdecimal_65_30%s, %sdecimal_65_0%s, %sdec%s          , %snumeric%s      , %sfloat%s          , %sfloat_z%s, %sfloat_53%s, %sreal%s, %sdouble%s                 , %sdouble_precision%s, %sbit%s, %sbit_5%s, %sbit_9%s, %sbit_64%s, %sdate%s       , %sdatetime%s            , %sdatetime_6%s          , %stimestamp%s           , %stimestamp_2%s         ,  %stime%s      , %stime_2%s     , %syear%s, %schar%s, %svarchar%s, %svarchar_def%s, %sbinary%s, %svarbinary%s, %stinyblob%s, %sblob%s, %smediumblob%s, %slongblob%s, %stinytext%s, %stext%s, %smediumtext%s, %slongtext%s, %senum%s , %sset%s, %sjson%s                                )
			VALUES
			(-128	    , -128           , 0            , 0            , -32768      , 0             , 0             , -8388608     , 0              , 0              , -2147483648   , 0            , 0            , -9223372036854775808   , 0                      , 0                      , 0        , '3.50'           , NULL            , NULL            , '3.50'           , '3.50'           , 1.175494351E-38    , NULL       , NULL        , NULL    , -1.7976931348623157E+308   , NULL                , 0      , 0        , NULL     , NULL      , '1970-01-01'   , '1000-01-01 00:00:00'   , '1000-01-01 00:00:00'   , '1970-01-01 03:00:01'   , '1970-01-01 03:00:01'   , '-838:59:59'   , '-838:59:59'   , '1901'  , 0       , ''         , ''             , ''        , ''           , ''          , ''      , ''            , ''          , ''          , ''      , ''            , ''          , '1'      , '1'    , '{}'                                    )
			,
			(127        , 127            , 255          , 255          , 32767       , 65535         , 65535         , 8388607      , 16777215       , 16777215       , 2147483647    , 4294967295   , 4294967295   , 9223372036854775807    , 18446744073709551615   , 18446744073709551615   , 1        , '12345678.1'     , NULL            , NULL            , '12345678.1'     , '12345678.1'     , 3.402823466E+7     , NULL       , NULL        , NULL    , -2.2250738585072014E-308   , NULL                , 1      , 31       , NULL     , NULL      , '2038-01-19'   , '9999-12-31 23:59:59'   , '9999-12-31 23:59:59'   , '2038-01-19 03:14:07'   , '2038-01-19 03:14:07'   , '838:59:59'    , '838:59:59'    , '2155'  , 255     , NULL       , NULL           , NULL      , NULL         , NULL        , NULL    , NULL          , NULL        , NULL        , NULL    , NULL          , NULL        , '3'      , '3'    , '{"a":"b"  , "c":1  , "d":{}  , "e":[]}')
			;
		`, "%s", "`")

	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	require.NoError(t, err)

	_, err = tx.Query(insertRequest)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	err = conn.Close()
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, "__test",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target),
		60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
