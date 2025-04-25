package connection_limit

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	mysql_client "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *helpers.RecipeMysqlSource()
	Target       = *helpers.RecipeMysqlTarget()
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestConnectionLimit(t *testing.T) {
	time.Sleep(5 * time.Second)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "MYSQL source", Port: Source.Port},
			helpers.LabeledPort{Label: "MYSQL target", Port: Target.Port},
		))
	}()
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
	defer conn.Close()

	_, err = db.ExecContext(context.Background(), "set global max_user_connections=3;")
	require.NoError(t, err)
	//------------------------------------------------------------------------------------
	// start worker

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	var terminateErr error
	localWorker := helpers.Activate(t, transfer, func(err error) {
		terminateErr = err
	})
	defer localWorker.Close(t)
	require.NoError(t, helpers.WaitDestinationEqualRowsCount(
		Target.Database,
		"some_table",
		helpers.GetSampleableStorageByModel(t, Target),
		60*time.Second,
		5,
	))
	require.NoError(t, terminateErr)
}
