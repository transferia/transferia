package snapshot

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
	ytcommon "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
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
		DBTables:  []string{"public.__test1"},
	}
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e").(*ytcommon.YtDestinationWrapper)
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func TestMain(m *testing.M) {
	ytcommon.InitExe()
	os.Exit(m.Run())
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
	Source.PreSteps.Constraint = true
	Target.Model.UseStaticTableOnSnapshot = true
	Target.Model.Cleanup = model.DisabledCleanup

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Snapshot", Snapshot)
	})
}

func Snapshot(t *testing.T) {
	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)

	_ = helpers.Activate(t, transfer)
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test1",
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))

	ctx := context.Background()
	srcConn, err := postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	_, err = srcConn.Exec(ctx, "INSERT INTO public.__test1 (id, name) VALUES (3, 'test1test1') on conflict do nothing ;")
	require.NoError(t, err)
	_, err = srcConn.Exec(ctx, "UPDATE public.__test1 SET name = 'test1test1' WHERE id = 1;")
	require.NoError(t, err)
	_, err = srcConn.Exec(ctx, "DELETE FROM public.__test1 WHERE id = 2;")
	require.NoError(t, err)

	_ = helpers.Activate(t, transfer)
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "__test1",
		helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second, 3))

	reader, err := ytEnv.YT.ReadTable(ctx, ypath.Path("//home/cdc/test/pg2yt_e2e/__test1"), nil)
	require.NoError(t, err)

	for reader.Next() {
		var row map[string]interface{}
		err = reader.Scan(&row)
		require.NoError(t, err)
		require.Contains(t, row, "id")

		if row["id"] == int64(1) {
			require.Equal(t, row["name"], "test1test1")
		}
	}
	require.NoError(t, reader.Err())
}
