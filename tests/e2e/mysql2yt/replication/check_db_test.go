package replication

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	ytMain "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	source = *helpers.WithMysqlInclude(helpers.RecipeMysqlSource(), []string{"__test", "__test_composite_pkey"})
	target = yt_helpers.RecipeYtTarget("//home/cdc/test/mysql2yt_e2e_replication")

	sourceDatabase        = os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE")
	tablePath             = ypath.Path(fmt.Sprintf("//home/cdc/test/mysql2yt_e2e_replication/%s___test", sourceDatabase))
	tableCompositeKeyPath = ypath.Path(fmt.Sprintf("//home/cdc/test/mysql2yt_e2e_replication/%s___test_composite_pkey", sourceDatabase))
)

func init() {
	source.WithDefaults()
}

func makeConnConfig() *mysqlDriver.Config {
	cfg := mysqlDriver.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", source.Host, source.Port)
	cfg.User = source.User
	cfg.Passwd = string(source.Password)
	cfg.DBName = source.Database
	cfg.Net = "tcp"
	return cfg
}

func TestGroup(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(target.Cluster())
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

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_replication"), ytMain.NodeMap, &ytMain.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_replication"), &ytMain.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	t.Run("Load", Load)
}

func closeReader(reader ytMain.TableReader) {
	err := reader.Close()
	if err != nil {
		logger.Log.Warn("Could not close table reader")
	}
}

func Load(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &source, target, abstract.TransferTypeSnapshotAndIncrement)

	ctx := context.Background()

	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err := snapshotLoader.LoadSnapshot(ctx)
	require.NoError(t, err)

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	initialReader, err := ytEnv.YT.ReadTable(ctx, tablePath, &ytMain.ReadTableOptions{})
	require.NoError(t, err)
	defer closeReader(initialReader)

	type row struct {
		ID    int    `yson:"id"`
		Value string `yson:"value"`
	}

	var i int
	for i = 0; initialReader.Next(); i++ {
		var row row
		err := initialReader.Scan(&row)
		require.NoError(t, err)
		switch i {
		case 0:
			require.EqualValues(t, 1, row.ID)
			require.EqualValues(t, "test", row.Value)
		case 1:
			require.EqualValues(t, 2, row.ID)
			require.EqualValues(t, "magic", row.Value)
		default:
			require.Fail(t, fmt.Sprintf("Unexpected item at position %d: %v", i, row))
		}
	}
	require.Equal(t, 2, i)

	compositeTableReader, err := ytEnv.YT.ReadTable(ctx, tableCompositeKeyPath, &ytMain.ReadTableOptions{})
	require.NoError(t, err)
	defer closeReader(compositeTableReader)

	type rowComposite struct {
		ID    int    `yson:"id"`
		ID2   int    `yson:"id2"`
		Value string `yson:"value"`
	}

	var j int
	for j = 0; compositeTableReader.Next(); j++ {
		var row rowComposite
		err := compositeTableReader.Scan(&row)
		require.NoError(t, err)
		switch j {
		case 0:
			require.EqualValues(t, 1, row.ID)
			require.EqualValues(t, 12, row.ID2)
			require.EqualValues(t, "test", row.Value)
		case 1:
			require.EqualValues(t, 2, row.ID)
			require.EqualValues(t, 22, row.ID2)
			require.EqualValues(t, "magic", row.Value)
		default:
			require.Fail(t, fmt.Sprintf("Unexpected item at position %d: %v", j, row))
		}
	}
	require.Equal(t, 2, j)

	fakeClient := coordinator.NewStatefulFakeClient()
	err = mysql.SyncBinlogPosition(&source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	conn, err := mysqlDriver.NewConnector(makeConnConfig())
	require.NoError(t, err)
	db := sql.OpenDB(conn)
	_, err = db.Exec("INSERT INTO `__test` (`id`, `value`) VALUES (3, 'stereo')")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO `__test_composite_pkey` (`id`, `id2`, `value`) VALUES (3, 32, 'stereo')")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO `__test` (`id`, `value`) VALUES (4, 'retroCarzzz')")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO `__test_composite_pkey` (`id`, `id2`, `value`) VALUES (4, 42, 'retroCarzzz')")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO `__test_composite_pkey` (`id`, `id2`, `value`) VALUES (5, 52, 'retroCarzzz')")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, source.Database, "__test", helpers.GetSampleableStorageByModel(t, source), helpers.GetSampleableStorageByModel(t, target.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, source.Database, "__test_composite_pkey", helpers.GetSampleableStorageByModel(t, source), helpers.GetSampleableStorageByModel(t, target.LegacyModel()), 60*time.Second))

	a := map[string]int{"id": 3}
	b := map[string]int{"id": 4}
	changesReader, err := ytEnv.YT.LookupRows(ctx, tablePath, []interface{}{a, b}, &ytMain.LookupRowsOptions{})
	require.NoError(t, err)
	defer closeReader(changesReader)

	for i = 0; changesReader.Next(); i++ {
		var row row
		err := changesReader.Scan(&row)
		require.NoError(t, err)
		if row.ID == 3 {
			require.EqualValues(t, row.Value, "stereo")
		} else {
			require.EqualValues(t, row.Value, "retroCarzzz")
		}
	}

	require.Equal(t, 2, i)

	_, err = db.Exec("UPDATE `__test_composite_pkey` SET `value` = 'updated' WHERE `id` = 1")
	require.NoError(t, err)
	_, err = db.Exec("UPDATE `__test_composite_pkey` SET `id2` = 23 WHERE `id` = 2")
	require.NoError(t, err)
	_, err = db.Exec("DELETE FROM `__test_composite_pkey` WHERE `id` = 5")
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, source.Database, "__test_composite_pkey", helpers.GetSampleableStorageByModel(t, source), helpers.GetSampleableStorageByModel(t, target.LegacyModel()), 60*time.Second))

	compositeTableReaderCheck, err := ytEnv.YT.SelectRows(ctx, fmt.Sprintf("* FROM [%v]", tableCompositeKeyPath), nil)
	require.NoError(t, err)
	defer closeReader(compositeTableReaderCheck)

	for j = 0; compositeTableReaderCheck.Next(); j++ {
		var row rowComposite
		err := compositeTableReaderCheck.Scan(&row)
		require.NoError(t, err)
		switch row.ID {
		case 1:
			require.EqualValues(t, row.Value, "updated")
			require.EqualValues(t, row.ID2, 12)
		case 2:
			require.EqualValues(t, row.Value, "magic")
			require.EqualValues(t, row.ID2, 23)
		case 3:
			require.EqualValues(t, row.Value, "stereo")
			require.EqualValues(t, row.ID2, 32)
		case 4:
			require.EqualValues(t, row.Value, "retroCarzzz")
			require.EqualValues(t, row.ID2, 42)
		}
	}
	require.Equal(t, 4, j)

	require.NoError(t, helpers.CompareStorages(t, source, target.LegacyModel(), helpers.NewCompareStorageParams()))
}
