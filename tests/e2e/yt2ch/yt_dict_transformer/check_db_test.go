package snapshot

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
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	ytprovider "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

const (
	TransferType            = abstract.TransferTypeSnapshotAndIncrement
	TransformedTableName    = "types_test"
	NotTransformedTableName = "types_test_not_transformed"
)

var (
	YtColumns, TestData = yt_helpers.YtTypesTestData()
	Source              = ytprovider.YtSource{
		Cluster: os.Getenv("YT_PROXY"),
		Proxy:   os.Getenv("YT_PROXY"),
		Paths: []string{
			fmt.Sprintf("//home/cdc/junk/%s", TransformedTableName),
			fmt.Sprintf("//home/cdc/junk/%s", NotTransformedTableName),
		},
	}
	Target = model.ChDestination{
		ShardsList:          []model.ClickHouseShard{{Name: "_", Hosts: []string{"localhost"}}},
		User:                "default",
		Database:            "default",
		HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		Cleanup:             dp_model.DisabledCleanup,
	}
	Timeout = 300 * time.Second
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	// to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
}

func initYTTable(t *testing.T) {
	ytc, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, &yt.Config{Proxy: Source.Proxy})
	require.NoError(t, err)
	opts := yt.WithCreateOptions(yt.WithSchema(schema.Schema{Columns: YtColumns}), yt.WithRecursive())
	for _, path := range Source.Paths {
		_ = ytc.RemoveNode(context.Background(), ypath.NewRich(path).YPath(), nil)
		wr, err := yt.WriteTable(context.Background(), ytc, ypath.NewRich(path).YPath(), opts)
		require.NoError(t, err)
		for _, row := range TestData {
			require.NoError(t, wr.Write(row))
		}
		require.NoError(t, wr.Commit())
	}
}

func initCHTable(t *testing.T) {
	storageParams, err := Target.ToStorageParams()
	require.NoError(t, err)
	chClient, err := httpclient.NewHTTPClientImpl(storageParams.ToConnParams())
	require.NoError(t, err)
	q := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, TransformedTableName)

	require.GreaterOrEqual(t, len(storageParams.ConnectionParams.Shards["_"]), 1)
	host := storageParams.ConnectionParams.Shards["_"][0]
	_ = chClient.Exec(context.Background(), logger.Log, host, q)
	q = fmt.Sprintf(`DROP TABLE IF EXISTS %s`, NotTransformedTableName)
	_ = chClient.Exec(context.Background(), logger.Log, host, q)
	// q = fmt.Sprintf(`CREATE TABLE types_test (%s) ENGINE MergeTree() ORDER BY id`, helpers.ChSchemaForYtTypesTestData())
	// require.NoError(t, chClient.Exec(context.Background(), logger.Log, Target.Shards()["_"][0], q))
}

func TestSnapshot(t *testing.T) {
	initYTTable(t)
	initCHTable(t)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	require.NoError(t, transfer.TransformationFromJSON(fmt.Sprintf(`{
		"transformers": [{
			"ytDictTransformer": {
				"tables": {
					"includeTables": [ "^.*%s$" ]
				}
			}
		}]
	}`, TransformedTableName)))

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	t.Run("Snapshot", Snapshot)

	t.Run("Canon", Canon)
}

type Response struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

func Snapshot(t *testing.T) {
	dst := helpers.GetSampleableStorageByModel(t, Target)
	n := uint64(1)
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("default", TransformedTableName, dst, Timeout, n))
	require.NoError(t, helpers.WaitDestinationEqualRowsCount("default", NotTransformedTableName, dst, Timeout, n))
}

func Canon(t *testing.T) {
	dst := helpers.GetSampleableStorageByModel(t, Target)
	var notTransformed, transformed []helpers.CanonTypedChangeItem

	desc := abstract.TableDescription{Schema: "default", Name: NotTransformedTableName}
	require.NoError(t, dst.LoadTable(context.Background(), desc, func(items []abstract.ChangeItem) error {
		notTransformed = append(notTransformed, helpers.ToCanonTypedChangeItems(items)...)
		return nil
	}))

	desc = abstract.TableDescription{Schema: "default", Name: TransformedTableName}
	require.NoError(t, dst.LoadTable(context.Background(), desc, func(items []abstract.ChangeItem) error {
		transformed = append(transformed, helpers.ToCanonTypedChangeItems(items)...)
		return nil
	}))

	canon.SaveJSON(t, map[string]interface{}{"not_transformed": notTransformed, "transformed": transformed})
}
