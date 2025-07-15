package snapshot

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	ytcommon "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

func TestClickhouseToYtStatic(t *testing.T) {
	src := &model.ChSource{
		ShardsList: []model.ClickHouseShard{
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
		HTTPPort:   helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort: helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
	}
	src.WithDefaults()

	dstModel := &ytcommon.YtDestination{
		Path:                     "//home/cdc/tests/e2e/pg2yt/yt_static",
		Cluster:                  os.Getenv("YT_PROXY"),
		CellBundle:               "default",
		PrimaryMedium:            "default",
		Static:                   false,
		DisableDatetimeHack:      true,
		UseStaticTableOnSnapshot: false, // this test is not supposed to work for static table
	}
	dst := &ytcommon.YtDestinationWrapper{Model: dstModel}
	dst.WithDefaults()

	t.Run("activate", func(t *testing.T) {
		transfer := helpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)
		require.NoError(t, tasks.ActivateDelivery(context.Background(), nil, coordinator.NewFakeClient(), *transfer, solomon.NewRegistry(solomon.NewRegistryOpts())))
		require.NoError(t, helpers.CompareStorages(t, src, dst.LegacyModel(), helpers.NewCompareStorageParams().WithEqualDataTypes(func(lDataType, rDataType string) bool {
			return true
		})))
	})
}
