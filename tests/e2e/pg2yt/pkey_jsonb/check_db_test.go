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
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
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
		Port:      testenv.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public.__test"},
		SlotID:    "test_slot_id",
	}
	Target = helpers_yt.RecipeYtTarget("//home/cdc/test/pg2yt_e2e_pkey_jsonb")
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
	targetPort, err := network.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ctx := context.Background()

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	_, err = ytEnv.YT.CreateNode(ctx, ypath.Path("//home/cdc/test/pg2yt_e2e_pkey_jsonb"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	defer func() {
		err := ytEnv.YT.RemoveNode(ctx, ypath.Path("//home/cdc/test/pg2yt_e2e_pkey_jsonb"), &yt.RemoveNodeOptions{Recursive: true})
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

func closeReader(reader yt.TableReader) {
	err := reader.Close()
	if err != nil {
		logger.Log.Warn("Could not close table reader")
	}
}

func checkContent(t *testing.T, tablePath ypath.Path) bool {
	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	changesReader, err := ytEnv.YT.SelectRows(context.Background(), fmt.Sprintf("* FROM [%s]", tablePath), &yt.SelectRowsOptions{})
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
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotAndIncrement)

	srcConnConfig, err := provider_postgres.MakeConnConfigFromSrc(logger.Log, &Source)
	require.NoError(t, err)
	srcConnConfig.PreferSimpleProtocol = true
	srcConn, err := provider_postgres.NewPgConnPool(srcConnConfig, nil)
	require.NoError(t, err)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	//------------------------------------------------------------------------------

	_, err = srcConn.Exec(context.Background(), "UPDATE public.__test SET v = v + 1;")
	require.NoError(t, err)

	_, err = srcConn.Exec(context.Background(), `INSERT INTO public.__test VALUES (5,'{}',5), (6,'{}',6)`)
	require.NoError(t, err)

	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "__test", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target.LegacyModel()), 60*time.Second))

	tablePath := ypath.Path(Target.Path()).Child(getTableName(abstract.TableDescription{Name: "__test"}))
	matched := checkContent(t, tablePath)
	require.True(t, matched)
}
