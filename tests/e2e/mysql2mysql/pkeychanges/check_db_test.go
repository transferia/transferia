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
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers"
	"github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *helpers.RecipeMysqlSource()
	Target       = *helpers.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
)

func init() {
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
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
	_, err := provider_mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = provider_mysql.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)
	helpers.Activate(t, transfer)
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
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

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)

	fakeClient := coordinator.NewStatefulFakeClient()
	err := provider_mysql.SyncBinlogPosition(&Source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	queries := []string{
		"update api_reports_offline set taskid = 10, ClientID = 1, ReportName = 'test' where taskid = 1",
		"update api_reports_offline set taskid = 20, ClientID = 2, ReportName = 'test' where taskid = 2",
		"update api_reports_offline set taskid = 30, ClientID = 3, ReportName = 'test' where taskid = 3",
		"delete from api_reports_offline where taskid = 10",
	}

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

	transaction, err := conn.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	for _, query := range queries {
		_, err = transaction.Exec(query)
		require.NoError(t, err)
	}

	err = transaction.Commit()
	require.NoError(t, err)

	err = conn.Close()
	require.NoError(t, err)

	sourceStorage := helpers.GetSampleableStorageByModel(t, Source)
	targetStorage := helpers.GetSampleableStorageByModel(t, Target)
	testTableName := "api_reports_offline"

	require.NoError(t, helpers.WaitEqualRowsCountDifferentSchemas(t,
		Source.Database, Target.Database, testTableName,
		sourceStorage,
		targetStorage,
		60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
