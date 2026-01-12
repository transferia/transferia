package jsonrootnumber

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	yt "go.ytsaurus.tech/yt/go/yt"
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
		DBTables:  []string{"public.root_number"},
		SlotID:    "test_slot_id",
	}
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e_json_root_number")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

type jsonRootNumberRow struct {
	ID     int64  `yson:"id"`
	Amount string `yson:"amount"`
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

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path(Target.Path()), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path(Target.Path()), &yt.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()

	t.Run("Load", func(t *testing.T) {
		Load(t, ytEnv)
	})
}

func Load(t *testing.T, ytEnv *yttest.Env) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)

	srcConnConfig, err := postgres.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	srcConnConfig.PreferSimpleProtocol = true
	srcConn, err := postgres.NewPgConnPool(srcConnConfig, nil)
	require.NoError(t, err)
	defer srcConn.Close()

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	require.NoError(t, helpers.WaitEqualRowsCount(
		t,
		"public",
		"root_number",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target.LegacyModel()),
		60*time.Second,
	))

	tableName := listFirstTable(t, ytEnv, Target.Path())
	got := readSingle(t, ytEnv, ypath.Path(Target.Path()).Child(tableName))

	const expected = "340791.17999998387"
	require.EqualValues(t, expected, got.Amount)
}

func listFirstTable(t *testing.T, ytEnv *yttest.Env, root string) string {
	var tables []struct {
		Name string `yson:",value"`
	}
	err := ytEnv.YT.ListNode(context.Background(), ypath.Path(root), &tables, nil)
	require.NoError(t, err)
	require.NotEmpty(t, tables, "no tables under %s", root)
	return tables[0].Name
}

func readSingle(t *testing.T, ytEnv *yttest.Env, table ypath.Path) jsonRootNumberRow {
	reader, err := ytEnv.YT.SelectRows(
		context.Background(),
		"* from ["+table.String()+"]",
		nil,
	)
	require.NoError(t, err)
	defer reader.Close()

	require.True(t, reader.Next(), "no rows in %s", table.String())
	var row jsonRootNumberRow
	require.NoError(t, reader.Scan(&row))
	return row
}
