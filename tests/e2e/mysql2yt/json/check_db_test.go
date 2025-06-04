package mysqltoytupdateminimal

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	ytcommon "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

const tableName = "test"

var (
	source        = *helpers.WithMysqlInclude(helpers.RecipeMysqlSource(), []string{tableName})
	targetCluster = os.Getenv("YT_PROXY")
)

func init() {
	source.WithDefaults()
}

func makeConnConfig() *mysql.Config {
	cfg := mysql.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", source.Host, source.Port)
	cfg.User = source.User
	cfg.Passwd = string(source.Password)
	cfg.DBName = source.Database
	cfg.Net = "tcp"
	return cfg
}

func makeTarget() ytcommon.YtDestinationModel {
	target := ytcommon.NewYtDestinationV1(ytcommon.YtDestination{
		Path:          "//home/cdc/test/mysql2yt/json",
		CellBundle:    "default",
		PrimaryMedium: "default",
		Cluster:       targetCluster,
	})
	target.WithDefaults()
	return target
}

type ytRow struct {
	ID   int `yson:"Id"`
	Data struct {
		Val string `yson:"val"`
	}
}

func TestUpdateMinimal(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(targetCluster)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt/json"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	ytDestination := makeTarget()
	transfer := helpers.MakeTransfer(helpers.TransferID, &source, ytDestination, abstract.TransferTypeSnapshotAndIncrement)
	wrkr := helpers.Activate(t, transfer)
	defer wrkr.Close(t)
	conn, err := mysql.NewConnector(makeConnConfig())
	require.NoError(t, err)

	requests := []string{
		"update test set Data = '{\"val\": 2}' where Id in (2);",
	}

	db := sql.OpenDB(conn)
	for _, request := range requests {
		_, err := db.Exec(request)
		require.NoError(t, err)
	}
	require.NoError(t, helpers.WaitEqualRowsCount(t, source.Database, "test", helpers.GetSampleableStorageByModel(t, source), helpers.GetSampleableStorageByModel(t, ytDestination.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, source, ytDestination.LegacyModel(), helpers.NewCompareStorageParams()))
	rows, err := ytEnv.YT.SelectRows(ctx, fmt.Sprintf(`* from [//home/cdc/test/mysql2yt/json/%v_test]`, source.Database), nil)
	require.NoError(t, err)

	var resRows []ytRow
	for rows.Next() {
		var r ytRow
		require.NoError(t, rows.Scan(&r))
		resRows = append(resRows, r)
	}
	logger.Log.Info("res", log.Any("res", resRows))
	require.Len(t, resRows, 3)
	for _, r := range resRows {
		require.Equal(t, fmt.Sprintf("%v", r.ID), r.Data.Val)
	}
}
