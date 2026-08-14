package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
	"github.com/transferia/transferia/tests/helpers"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

var (
	TransferType        = abstract.TransferTypeSnapshotOnly
	YtColumns, TestData = helpers_yt.YtTypesTestData()
	Source              = provider_yt.YtSource{
		Cluster: os.Getenv("YT_PROXY"),
		YtProxy: os.Getenv("YT_PROXY"),
		Paths:   []string{"//home/cdc/junk/types_test"},
		YtToken: "",
	}
	Target = clickhouse_model.ChDestination{
		ShardsList:          []clickhouse_model.ClickHouseShard{{Name: "_", Hosts: []string{"localhost"}}},
		User:                "default",
		Password:            "",
		Database:            "default",
		HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		SSLEnabled:          false,
		Cleanup:             model.DisabledCleanup,
	}
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	// to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
}

func initYTTable(t *testing.T) {
	ytc, err := yt_client.NewYtClientWrapper(yt_client.HTTP, nil, &yt.Config{Proxy: Source.YtProxy})
	require.NoError(t, err)
	_ = ytc.RemoveNode(context.Background(), ypath.NewRich(Source.Paths[0]).YPath(), nil)

	sch := ytschema.Schema{
		Strict:     nil,
		UniqueKeys: false,
		Columns:    YtColumns,
	}

	opts := yt.WithCreateOptions(yt.WithSchema(sch), yt.WithRecursive())
	wr, err := yt.WriteTable(context.Background(), ytc, ypath.NewRich(Source.Paths[0]).YPath(), opts)
	require.NoError(t, err)
	for _, row := range TestData {
		require.NoError(t, wr.Write(row))
	}
	require.NoError(t, wr.Commit())
}

func initCHTable(t *testing.T) {
	storageParams, err := Target.ToStorageParams()
	require.NoError(t, err)
	chClient, err := httpclient.NewHTTPClientImpl(storageParams.ToConnParams())
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(storageParams.ConnectionParams.Shards["_"]), 1)
	host := storageParams.ConnectionParams.Shards["_"][0]

	q := `DROP TABLE IF EXISTS types_test`
	_ = chClient.Exec(context.Background(), logger.Log, host, q, nil)

	q = fmt.Sprintf(`CREATE TABLE types_test (%s) ENGINE MergeTree() ORDER BY id`, helpers_yt.ChSchemaForYtTypesTestData())
	require.NoError(t, chClient.Exec(context.Background(), logger.Log, host, q, nil))
}

func TestSnapshot(t *testing.T) {
	initYTTable(t)
	initCHTable(t)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	transfer.Labels = `{"dt-async-ch": "on"}`

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	chTarget := helpers.GetSampleableStorageByModel(t, Target)
	rowCnt := 0
	var targetItems []helpers.CanonTypedChangeItem
	require.NoError(t, chTarget.LoadTable(context.Background(), abstract.TableDescription{
		Name:   "types_test",
		Schema: "default",
	}, func(input []abstract.ChangeItem) error {
		for _, ci := range input {
			switch ci.Kind {
			case abstract.InitTableLoad, abstract.DoneTableLoad:
				continue
			case abstract.InsertKind:
				targetItems = append(targetItems, helpers.ToCanonTypedChangeItem(ci))
				rowCnt++
			default:
				return xerrors.Errorf("unexpected ChangeItem kind %s", string(ci.Kind))
			}
		}
		return nil
	}))

	require.Equal(t, len(TestData), rowCnt)
	canon.SaveJSON(t, targetItems)
}
