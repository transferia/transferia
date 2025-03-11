package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	dp_model "github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/clickhouse/httpclient"
	"github.com/transferria/transferria/pkg/providers/clickhouse/model"
	ytprovider "github.com/transferria/transferria/pkg/providers/yt"
	ytclient "github.com/transferria/transferria/pkg/providers/yt/client"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/tests/helpers"
	yt_helpers "github.com/transferria/transferria/tests/helpers/yt"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

var (
	TransferType        = abstract.TransferTypeSnapshotOnly
	YtColumns, TestData = yt_helpers.YtTypesTestData()
	Source              = ytprovider.YtSource{
		Cluster: os.Getenv("YT_PROXY"),
		Proxy:   os.Getenv("YT_PROXY"),
		Paths:   []string{"//home/cdc/junk/types_test"},
		YtToken: "",
	}
	Target = model.ChDestination{
		ShardsList:          []model.ClickHouseShard{{Name: "_", Hosts: []string{"localhost"}}},
		User:                "default",
		Password:            "",
		Database:            "default",
		HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		SSLEnabled:          false,
		Cleanup:             dp_model.DisabledCleanup,
	}
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	// to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
}

func initYTTable(t *testing.T) {
	ytc, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, &yt.Config{Proxy: Source.Proxy})
	require.NoError(t, err)
	_ = ytc.RemoveNode(context.Background(), ypath.NewRich(Source.Paths[0]).YPath(), nil)

	sch := schema.Schema{
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
	chClient, err := httpclient.NewHTTPClientImpl(Target.ToStorageParams().ToConnParams())
	require.NoError(t, err)

	q := `DROP TABLE IF EXISTS types_test`
	_ = chClient.Exec(context.Background(), logger.Log, Target.Shards()["_"][0], q)

	q = fmt.Sprintf(`CREATE TABLE types_test (%s) ENGINE MergeTree() ORDER BY id`, yt_helpers.ChSchemaForYtTypesTestData())
	require.NoError(t, chClient.Exec(context.Background(), logger.Log, Target.Shards()["_"][0], q))
}

func TestSnapshot(t *testing.T) {
	initYTTable(t)
	initCHTable(t)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	transfer.Labels = `{"dt-async-ch": "on"}`
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	require.NoError(t, snapshotLoader.UploadV2(context.Background(), nil, nil))

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
