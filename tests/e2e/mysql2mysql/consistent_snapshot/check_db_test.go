package geometry_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	mysql_client "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/pkg/storage"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
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

	t.Run("Main group", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Snapshot", Snapshot)
	})
}

func Existence(t *testing.T) {
	_, err := mysql.NewStorage(Source.ToStorageParams())
	require.NoError(t, err)
	_, err = mysql.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func dropData(t *testing.T) {
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

	requests := []string{
		`delete from fruit`,
		`delete from employee`,
	}

	for _, request := range requests {
		rows, err := conn.QueryContext(context.Background(), request)
		require.NoError(t, err)
		require.NoError(t, rows.Close())
	}

	err = conn.Close()
	require.NoError(t, err)
}

func checkTarget(t *testing.T) {
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

	var count int

	err = conn.QueryRowContext(context.Background(), "select count(*) from fruit").Scan(&count)
	require.NoError(t, err)
	require.EqualValues(t, 12, count)

	err = conn.QueryRowContext(context.Background(), "select count(*) from employee").Scan(&count)
	require.NoError(t, err)
	require.EqualValues(t, 8, count)

	err = conn.Close()
	require.NoError(t, err)
}

func Snapshot(t *testing.T) {
	Source.ConsistentSnapshot = true
	Source.SnapshotDegreeOfParallelism = 1

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, abstract.TransferTypeSnapshotOnly)
	transfer = helpers.WithLocalRuntime(transfer, 1, 1)

	storage, err := storage.NewStorage(transfer, coordinator.NewFakeClient(), helpers.EmptyRegistry())
	require.NoError(t, err)
	defer storage.Close()
	mysqlStorage, ok := storage.(*mysql.Storage)
	require.True(t, ok)
	tables, err := model.FilteredTableList(storage, transfer)
	require.NoError(t, err)

	err = mysqlStorage.BeginSnapshot(context.TODO())
	require.NoError(t, err)

	dropData(t)

	operationID := "test-operation"

	operationTables := []*model.OperationTablePart{}
	for _, table := range tables.ConvertToTableDescriptions() {
		operationTables = append(operationTables, model.NewOperationTablePartFromDescription(operationID, &table))
	}

	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), operationID, transfer, helpers.EmptyRegistry())

	partProvider := tasks.NewLocalTablePartProvider(operationTables...)
	err = snapshotLoader.DoUploadTables(context.TODO(), storage, partProvider.TablePartProvider())
	require.NoError(t, err)

	err = mysqlStorage.EndSnapshot(context.TODO())
	require.NoError(t, err)

	checkTarget(t)
}
