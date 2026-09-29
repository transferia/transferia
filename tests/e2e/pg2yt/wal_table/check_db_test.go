package canonreplication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	transformerhelpers "github.com/transferia/transferia/tests/helpers/transformer"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	source = pgrecipe.RecipeSource(
		pgrecipe.WithDBTables("public.test"),
		pgrecipe.WithInitDir("dump"),
		pgrecipe.WithPrefix(""))
	target = provider_yt.NewYtDestinationV1(*helpers_yt.SetRecipeYt(&provider_yt.YtDestination{
		Path:    "//home/cdc/test/pg2yt_e2e_wal",
		PushWal: true,
	}))
)

func TestGroup(t *testing.T) {
	target.WithDefaults()

	targetPort, err := network.GetPortFromStr(target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: source.Port},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	t.Run("Load", Load)
}

func Load(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)

	commitTime := uint64(1714117589532851000)
	lsn := uint64(1000)
	txID := uint32(1)
	fixLSN := func(_ *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
		items = yslices.Filter(items, func(item abstract.ChangeItem) bool {
			return !abstract.IsSystemTable(item.Table)
		})
		for i := 0; i < len(items); i++ {
			if items[i].CommitTime != 0 {
				items[i].CommitTime = commitTime
			}
			if items[i].LSN != 0 {
				items[i].LSN = lsn
			}
			if items[i].ID != 0 {
				items[i].ID = txID
			}
			commitTime++
			lsn++
			txID++
		}
		return abstract.TransformerResult{
			Transformed: items,
			Errors:      nil,
		}
	}

	lsnTransformer := transformerhelpers.NewSimpleTransformer(t, fixLSN, func(abstract.TableID, abstract.TableColumns) bool { return true })
	transformerhelpers.AddTransformer(t, transfer, lsnTransformer)
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	//------------------------------------------------------------------------------

	ctx := context.Background()
	srcConn, err := provider_postgres.MakeConnPoolFromSrc(source, logger.Log)
	require.NoError(t, err)

	_, err = srcConn.Exec(ctx, `INSERT INTO public.test (str, id, aid, da, enum_v, empty_arr, int4_arr, text_arr, enum_arr, json_arr, char_arr, udt_arr) VALUES ('badabums', 911,  1,'2011-09-11', 'happy', '{}', '{1, 2, 3}', '{"foo", "bar"}', '{"sad", "ok"}', ARRAY['{}', '{"foo": "bar"}', '{"arr": [1, 2, 3]}']::json[], '{"a", "b", "c"}', ARRAY['("city1","street1")'::full_address, '("city2","street2")'::full_address]) on conflict do nothing ;`)
	require.NoError(t, err)
	_, err = srcConn.Exec(ctx, `INSERT INTO public.test (str, id, aid, da, enum_v, int4_arr, text_arr, char_arr) VALUES ('badabums', 911, 1,'2011-09-11', 'sad', '[1:1][3:4][3:5]={{{1,2,3},{4,5,6}}}', '{{"foo", "bar"}, {"abc", "xyz"}}', '{"x", "y", "z"}') on conflict do nothing ;`)
	require.NoError(t, err)

	_, err = srcConn.Exec(ctx, `UPDATE public.test SET id = 1000 WHERE str = 'this should be updated';`)
	require.NoError(t, err)
	_, err = srcConn.Exec(ctx, `DELETE FROM public.test WHERE str = 'this should be deleted';`)
	require.NoError(t, err)

	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "test", storagecomparison.GetSampleableStorageByModel(t, source), storagecomparison.GetSampleableStorageByModel(t, target.LegacyModel()), 60*time.Second))

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	helpers_yt.CanonizeDynamicYtTable(t, ytEnv.YT, ypath.Path(target.Path()).Child("__wal"), "__wal.json")
}
