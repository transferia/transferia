package canonreplication

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	Source = provider_postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public.__test"},
		SlotID:    "test_slot_id",
	}
	Target = helpers_yt.RecipeYtTarget("//home/cdc/test/pg2yt_e2e_replication_canon")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	_ = os.Setenv("TZ", "Europe/Moscow")
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

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path(Target.Path()), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path(Target.Path()), &yt.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	t.Run("Load", Load)
}

func Load(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotAndIncrement)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//------------------------------------------------------------------------------

	ctx := context.Background()
	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)

	_, err = srcConn.Exec(ctx, `INSERT INTO public.__test (str, id, aid, da, enum_v, empty_arr, int4_arr, text_arr, enum_arr, json_arr, char_arr, udt_arr) VALUES ('badabums', 911,  1,'2011-09-11', 'happy', '{}', '{1, 2, 3}', '{"foo", "bar"}', '{"sad", "ok"}', ARRAY['{}', '{"foo": "bar"}', '{"arr": [1, 2, 3]}']::json[], '{"a", "b", "c"}', ARRAY['("city1","street1")'::full_address, '("city2","street2")'::full_address]) on conflict do nothing ;`)
	require.NoError(t, err)
	_, err = srcConn.Exec(ctx, `INSERT INTO public.__test (str, id, aid, da, enum_v, int4_arr, text_arr, char_arr) VALUES ('badabums', 911, 1,'2011-09-11', 'sad', '[1:1][3:4][3:5]={{{1,2,3},{4,5,6}}}', '{{"foo", "bar"}, {"abc", "xyz"}}', '{"x", "y", "z"}') on conflict do nothing ;`)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	rows, err := ytEnv.YT.SelectRows(
		ctx,
		fmt.Sprintf("* from [%v/__test]", Target.Path()),
		nil,
	)
	require.NoError(t, err)
	var result []map[string]interface{}
	for rows.Next() {
		require.NoError(t, rows.Err())
		var res map[string]interface{}
		require.NoError(t, rows.Scan(&res))
		result = append(result, res)
	}
	canon.SaveJSON(t, result)
}
