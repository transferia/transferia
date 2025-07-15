package pkeyjsonb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	yt_main "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	Source = postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public.__test"},
		SlotID:    "test_slot_id",
	}
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e_pkey_jsonb")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

type row struct {
	ID    int    `yson:"id"`
	JSONB string `yson:"jb"`
	Value int    `yson:"v"`
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

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/pg2yt_e2e_pkey_jsonb"), yt_main.NodeMap, &yt_main.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/pg2yt_e2e_pkey_jsonb"), &yt_main.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	t.Run("Load", Load)
}

func getTableName(t abstract.TableDescription) string {
	if t.Schema == "" || t.Schema == "public" {
		return t.Name
	}

	return t.Schema + "_" + t.Name
}

func closeReader(reader yt_main.TableReader) {
	err := reader.Close()
	if err != nil {
		logger.Log.Warn("Could not close table reader")
	}
}

func checkContent(t *testing.T, tablePath ypath.Path) bool {
	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	changesReader, err := ytEnv.YT.SelectRows(context.Background(), fmt.Sprintf("* FROM [%s]", tablePath), &yt_main.SelectRowsOptions{})
	require.NoError(t, err)
	defer closeReader(changesReader)

	rows := 0
	correct := 0
	for changesReader.Next() {
		var row row
		err := changesReader.Scan(&row)
		require.NoError(t, err)

		if row.ID < 1 || row.ID > 3 {
			continue
		}

		rows++

		if row.Value == row.ID+1 {
			correct++
		}
	}

	require.EqualValues(t, rows, 3)

	return correct == 3
}

func Load(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotAndIncrement)

	srcConnConfig, err := postgres.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	srcConnConfig.PreferSimpleProtocol = true
	srcConn, err := postgres.NewPgConnPool(srcConnConfig, nil)
	require.NoError(t, err)

	//------------------------------------------------------------------------------

	err = postgres.CreateReplicationSlot(&Source)
	require.NoError(t, err)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	//------------------------------------------------------------------------------

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	//------------------------------------------------------------------------------

	_, err = srcConn.Exec(context.Background(), "UPDATE public.__test SET v = v + 1;")
	require.NoError(t, err)

	_, err = srcConn.Exec(context.Background(), `INSERT INTO public.__test VALUES (5,'{}',5), (6,'{}',6)`)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))

	tablePath := ypath.Path(Target.Path()).Child(getTableName(abstract.TableDescription{Name: "__test"}))
	matched := checkContent(t, tablePath)
	require.True(t, matched)
}
