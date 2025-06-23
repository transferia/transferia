package light

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	mysql_client "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

func TestSnapshotAndReplicationViewsCompatibility(t *testing.T) {
	source := *helpers.RecipeMysqlSource()
	source.PreSteps.View = true
	target := *helpers.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "Mysql source", Port: source.Port},
		helpers.LabeledPort{Label: "Mysql target", Port: target.Port},
	))
	transfer := helpers.MakeTransfer("fake", &source, &target, abstract.TransferTypeSnapshotAndIncrement)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	require.NoError(t, helpers.CompareStorages(t, source, target, helpers.NewCompareStorageParams()))

	requests := []string{
		"update test set name = 'Test Name' where id = 1;",
		"insert into test2(name, email, age) values ('name2', 'email2', 44);",
	}

	cfg := mysql_client.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", source.Host, source.Port)
	cfg.User = source.User
	cfg.Passwd = string(source.Password)
	cfg.DBName = source.Database
	cfg.Net = "tcp"

	mysqlConnector, err := mysql_client.NewConnector(cfg)
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
	require.NoError(t, helpers.CompareStorages(t, source, target, helpers.NewCompareStorageParams()))
}
