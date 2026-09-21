package snapshot

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

func TestClickhouseToYtStatic(t *testing.T) {
	src := &clickhouse_model.ChSource{
		ShardsList: []clickhouse_model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		User:       "default",
		Password:   "",
		Database:   "mtmobproxy",
		HTTPPort:   testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort: testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
	}
	src.WithDefaults()

	dstModel := &provider_yt.YtDestination{
		Path:                     "//home/cdc/tests/e2e/pg2yt/yt_static",
		Cluster:                  os.Getenv("YT_PROXY"),
		CellBundle:               "default",
		PrimaryMedium:            "default",
		Static:                   false,
		DisableDatetimeHack:      true,
		UseStaticTableOnSnapshot: false, // this test is not supposed to work for static table
	}
	dst := &provider_yt.YtDestinationWrapper{Model: dstModel}
	dst.WithDefaults()

	t.Run("activate", func(t *testing.T) {
		transfer := transferhelpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)
		require.NoError(t, tasks.ActivateDelivery(context.Background(), nil, coordinator.NewFakeClient(), *transfer, solomon.NewRegistry(solomon.NewRegistryOpts())))
		require.NoError(t, storagecomparison.CompareStorages(t, src, dst.LegacyModel(), storagecomparison.NewCompareStorageParams().WithEqualDataTypes(func(lDataType, rDataType string) bool {
			return true
		})))
	})
}
