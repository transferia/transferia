package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
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
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e").(*yt_provider.YtDestinationWrapper)
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
	Source.PreSteps.Constraint = true
	Target.Model.UseStaticTableOnSnapshot = true

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Snapshot", Snapshot)
		t.Run("SnapshotAndIncrement", SnapshotAndIncrement)
	})
}

func Snapshot(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)

	_ = helpers.Activate(t, transfer)
	checkStorages(t, "__test1")
}

func SnapshotAndIncrement(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotAndIncrement)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//------------------------------------------------------------------------------

	makeIncrementActions(t, "__test1")

	checkStorages(t, "__test1")
}

func makeIncrementActions(t *testing.T, table string) {
	ctx := context.Background()
	srcConn, err := postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)

	_, err = srcConn.Exec(ctx, fmt.Sprintf("INSERT INTO public.%s (id, name) VALUES (1000, 'test1test1') on conflict do nothing ;", table))
	require.NoError(t, err)
	_, err = srcConn.Exec(ctx, fmt.Sprintf("INSERT INTO public.%s (id, name) VALUES (2000, 'test2test2') on conflict do nothing ;", table))
	require.NoError(t, err)

	_, err = srcConn.Exec(ctx, fmt.Sprintf("UPDATE public.%s SET id = 1001 WHERE name = 'test1test1';", table))
	require.NoError(t, err)
	_, err = srcConn.Exec(ctx, fmt.Sprintf("DELETE FROM public.%s WHERE name = 'xxxxxxxxx';", table))
	require.NoError(t, err)
}

func checkStorages(t *testing.T, table string) {
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", table,
		helpers.GetSampleableStorageByModel(t, Source),
		helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))

	tableID := abstract.TableID{Namespace: "", Name: table}
	schema, err := helpers.GetSampleableStorageByModel(t, Target.LegacyModel()).TableSchema(context.Background(), tableID)
	require.NoError(t, err)
	require.Equal(t, 3, len(schema.ColumnNames()))
	require.Contains(t, schema.ColumnNames(), "__dummy")
}
