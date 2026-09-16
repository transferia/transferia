package replication

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	mysql_driver2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/mysql"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	"github.com/transferia/transferia/tests/helpers/transfer"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	source = *mysql.WithMysqlInclude(mysql.RecipeMysqlSource(), []string{"__test", "__test_composite_pkey"})
	target = helpers_yt.RecipeYtTarget("//home/cdc/test/mysql2yt_e2e_replication")

	sourceDatabase        = os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE")
	tablePath             = ypath.Path(fmt.Sprintf("//home/cdc/test/mysql2yt_e2e_replication/%s___test", sourceDatabase))
	tableCompositeKeyPath = ypath.Path(fmt.Sprintf("//home/cdc/test/mysql2yt_e2e_replication/%s___test_composite_pkey", sourceDatabase))
)

func init() {
	source.WithDefaults()
}

func makeConnConfig() *mysql_driver2.Config {
	cfg := mysql_driver2.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", source.Host, source.Port)
	cfg.User = source.User
	cfg.Passwd = string(source.Password)
	cfg.DBName = source.Database
	cfg.Net = "tcp"
	return cfg
}

func TestGroup(t *testing.T) {
	targetPort, err := network.GetPortFromStr(target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Mysql source", Port: source.Port},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_replication"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/mysql2yt_e2e_replication"), &yt.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	t.Run("Load", Load)
}

func closeReader(reader yt.TableReader) {
	err := reader.Close()
	if err != nil {
		logger.Log.Warn("Could not close table reader")
	}
}

func Load(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &source, target, abstract.TransferTypeSnapshotAndIncrement)

	ctx := context.Background()

	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), &model.TransferOperation{}, transfer, testmetrics.EmptyRegistry())
	err := snapshotLoader.LoadSnapshot(ctx)
	require.NoError(t, err)

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	initialReader, err := ytEnv.YT.ReadTable(ctx, tablePath, &yt.ReadTableOptions{})
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

	compositeTableReader, err := ytEnv.YT.ReadTable(ctx, tableCompositeKeyPath, &yt.ReadTableOptions{})
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
	err = provider_mysql.SyncBinlogPosition(&source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, transfer, testmetrics.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	conn, err := mysql_driver2.NewConnector(makeConnConfig())
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

	require.NoError(t, storage.WaitEqualRowsCount(t, source.Database, "__test", storagecomparison.GetSampleableStorageByModel(t, source), storagecomparison.GetSampleableStorageByModel(t, target.LegacyModel()), 60*time.Second))
	require.NoError(t, storage.WaitEqualRowsCount(t, source.Database, "__test_composite_pkey", storagecomparison.GetSampleableStorageByModel(t, source), storagecomparison.GetSampleableStorageByModel(t, target.LegacyModel()), 60*time.Second))

	a := map[string]int{"id": 3}
	b := map[string]int{"id": 4}
	changesReader, err := ytEnv.YT.LookupRows(ctx, tablePath, []interface{}{a, b}, &yt.LookupRowsOptions{})
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

	require.NoError(t, storage.WaitEqualRowsCount(t, source.Database, "__test_composite_pkey", storagecomparison.GetSampleableStorageByModel(t, source), storagecomparison.GetSampleableStorageByModel(t, target.LegacyModel()), 60*time.Second))

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

	require.NoError(t, storagecomparison.CompareStorages(t, source, target.LegacyModel(), storagecomparison.NewCompareStorageParams()))
}
