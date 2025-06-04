package snapshot

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	pg_provider "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	yt_main "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

const ytPath = "//home/cdc/test/pg2yt_e2e"

var (
	Source = pg_provider.PgSource{
		ClusterID:                   os.Getenv("PG_CLUSTER_ID"),
		Hosts:                       []string{"localhost"},
		User:                        os.Getenv("PG_LOCAL_USER"),
		Password:                    model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:                    os.Getenv("PG_LOCAL_DATABASE"),
		Port:                        helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:                    []string{"public.__test"},
		SnapshotDegreeOfParallelism: 4,
		DesiredTableSize:            uint64(100),
	}
	Target = yt_helpers.RecipeYtTarget(ytPath)
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestGroup(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	ctx := context.Background()
	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path(ytPath), yt_main.NodeMap, &yt_main.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path(ytPath), &yt_main.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Snapshot", Snapshot)
	})
}

func Snapshot(t *testing.T) {
	Source.PreSteps.Constraint = true
	transfer := helpers.MakeTransferForIncrementalSnapshot(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly,
		"public", "__test", "id", "", 15)

	fakeClient := coordinator.NewStatefulFakeClient()

	//------------------------------------------------------------------------------
	removeAddedData(t)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	snapshotLoader := tasks.NewSnapshotLoader(fakeClient, "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second), "Wrong row number after first snapshot round!")

	conn, err := pg_provider.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer conn.Close()

	addSomeData(t, conn)
	done := addSomeConcurrentDataAsyncWithDelay(t, 15, conn)

	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)
	logger.Log.Infof("Done loading data %v", <-done)

	expectedYtRows := getExpectedRowsCount(t, conn)
	storage := helpers.GetSampleableStorageByModel(t, Target.LegacyModel())
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "__test", storage, 60*time.Second, expectedYtRows), "Wrong row number after full increment round!")

	ids := readIdsFromTarget(t, storage)

	require.Contains(t, ids, int64(16), "Id 16 should be loaded!!")
	require.Contains(t, ids, int64(18), "Id 18 should be loaded!!")
	require.NotContains(t, ids, int64(20), "Id 20 should not be loaded during current increment cycle!")

	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second), "Wrong row number after first snapshot round!")

	ids = readIdsFromTarget(t, storage)
	require.Contains(t, ids, int64(20), "Id 20 should be loaded during last increment cycle!")
	removeAddedData(t)
}

func readIdsFromTarget(t *testing.T, storage abstract.SampleableStorage) []int64 {
	ids := make([]int64, 0)

	require.NoError(t, storage.LoadTable(context.Background(), abstract.TableDescription{
		Name:   "__test",
		Schema: "",
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	}, func(items []abstract.ChangeItem) error {
		for _, row := range items {
			if !row.IsRowEvent() {
				continue
			}
			id := row.ColumnNameIndex("id")
			ids = append(ids, row.ColumnValues[id].(int64))
		}
		return nil
	}))
	return ids
}

func getExpectedRowsCount(t *testing.T, conn *pgxpool.Pool) uint64 {
	var cnt uint64

	err := conn.QueryRow(context.Background(), "select count(*) from __test").Scan(&cnt)
	require.NoError(t, err, "Cannot get rows count")

	return cnt - 1 //should not get last inserted row
}

func removeAddedData(t *testing.T) {
	conn, err := pg_provider.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), "delete from __test where id >= 14")
	require.NoError(t, err)
}

func addSomeData(t *testing.T, conn *pgxpool.Pool) {
	logger.Log.Info("Will add some data after snapshot...")
	_, err := conn.Exec(context.Background(), "insert into __test (str, id, da, i) values ('qqq', 14, '1999-09-16', 1)")
	require.NoError(t, err)
	_, err = conn.Exec(context.Background(), `insert into __test (str, id, da, i) values
                                                      ('www', 15, '1999-09-16', 1),
                                                      ('eee', 17, '1999-09-16', 1),
                                                      ('rrr', 19, '1999-09-16', 1) `)
	require.NoError(t, err)
}

func addSomeConcurrentDataAsyncWithDelay(t *testing.T, delay int64, conn *pgxpool.Pool) chan bool {
	r := make(chan bool)
	go func() {
		logger.Log.Info("Will add some data asynchronously...")
		logger.Log.Info("Start adding some late concurrent data with sleep")
		query := "" +
			"begin;" +
			"insert into __test (str, id, da, i) values" +
			"    ('late data', 18, '2022-09-16', 1)," +
			"    ('late data 2', 16, '2022-10-16', 1)," +
			"    ('late data 3', 20, '2022-09-17', 1);" +
			"SELECT pg_sleep(" + strconv.FormatInt(delay-5, 10) + ");" +
			"commit;"
		_, err := conn.Exec(context.Background(), query)
		require.NoError(t, err)
		logger.Log.Info("Adding late data done!")
		r <- true
	}()
	return r
}
