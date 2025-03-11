package snapshot

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/providers/clickhouse/model"
	ytcommon "github.com/transferria/transferria/pkg/providers/yt"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/tests/helpers"
)

func TestMain(m *testing.M) {
	ytcommon.InitExe()
	os.Exit(m.Run())
}

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
