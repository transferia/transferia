package relocatortrigger

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	yt_main "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	srcPort = helpers.GetIntFromEnv("PG_LOCAL_PORT")
	Source  = postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      srcPort,
		DBTables:  []string{"public.wild_pokemon", "public.captured_pokemon"},
		SlotID:    "test_slot_id",
	}
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e_relocator_trigger")
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

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/pg2yt_e2e_relocator_trigger"), yt_main.NodeMap, &yt_main.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/pg2yt_e2e_relocator_trigger"), &yt_main.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	t.Run("Load", Load)
}

func Load(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotAndIncrement)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//------------------------------------------------------------------------------

	conn, err := postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)

	// Following queries should trigger row relocation to different table (see trigger in SQL file)
	_, err = conn.Exec(context.Background(), `UPDATE wild_pokemon
    SET home = 'Pokeball'
    WHERE name = 'Squirtle' OR name = 'Bulbasaur';`)
	require.NoError(t, err)

	_, err = conn.Exec(context.Background(), `UPDATE wild_pokemon
    SET home = 'Ultraball'
    WHERE id = 6;`)
	require.NoError(t, err)

	time.Sleep(time.Second)

	sampleableStorage := helpers.GetSampleableStorageByModel(t, Source)
	// Check row count in source after trigger
	rowsInWild, err := sampleableStorage.ExactTableRowsCount(
		abstract.TableID{
			Namespace: "public",
			Name:      "wild_pokemon",
		},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(6), rowsInWild)
	rowsInCaptured, err := sampleableStorage.ExactTableRowsCount(
		abstract.TableID{
			Namespace: "public",
			Name:      "captured_pokemon",
		},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(4), rowsInCaptured)

	// Check that Bulbasaur was correctly moved from wild to captured
	name := ""
	err = conn.QueryRow(context.Background(), "select name from captured_pokemon where id=$1", 1).Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Bulbasaur", name)

	// Check that the same changes were applied to target
	require.NoError(t, backoff.Retry(func() error {
		return helpers.CompareStorages(t, Source, Target.LegacyModel(), helpers.NewCompareStorageParams())
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second*5), 30)))
}
