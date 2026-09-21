package light

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	mysql_driver2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/mysql"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

func TestSnapshotAndReplicationViewsCompatibility(t *testing.T) {
	source := *mysql.RecipeMysqlSource()
	source.PreSteps.View = true
	target := *mysql.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
	defer require.NoError(t, network.CheckConnections(
		network.LabeledPort{Label: "Mysql source", Port: source.Port},
		network.LabeledPort{Label: "Mysql target", Port: target.Port},
	))
	transfer := transferhelpers.MakeTransfer("fake", &source, &target, abstract.TransferTypeSnapshotAndIncrement)
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	require.NoError(t, storagecomparison.CompareStorages(t, source, target, storagecomparison.NewCompareStorageParams()))

	requests := []string{
		"update test set name = 'Test Name' where id = 1;",
		"insert into test2(name, email, age) values ('name2', 'email2', 44);",
	}

	cfg := mysql_driver2.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", source.Host, source.Port)
	cfg.User = source.User
	cfg.Passwd = string(source.Password)
	cfg.DBName = source.Database
	cfg.Net = "tcp"

	mysqlConnector, err := mysql_driver2.NewConnector(cfg)
	require.NoError(t, err)
	db := sql.OpenDB(mysqlConnector)

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	for _, request := range requests {
		rows, err := conn.QueryContext(context.Background(), request)
		require.NoError(t, err)
		require.NoError(t, rows.Close())
	}

	err = conn.Close()
	require.NoError(t, err)
	require.NoError(t, storagecomparison.CompareStorages(t, source, target, storagecomparison.NewCompareStorageParams()))
}
