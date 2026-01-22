package ytstatic

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

type testTableRow struct {
	ID int `yson:"id"`
}

func TestYTStatic(t *testing.T) {
	ctx := context.Background()
	lgr := logger.Log

	ytEnv, cancel := yttest.NewEnv(t)
	defer cancel()

	src := &postgres.PgSource{
		Hosts:    []string{"localhost"},
		User:     os.Getenv("PG_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database: os.Getenv("PG_LOCAL_DATABASE"),
		Port:     helpers.GetIntFromEnv("PG_LOCAL_PORT"),
	}
	src.WithDefaults()

	dstModel := &yt_provider.YtDestination{
		Path:          "//home/cdc/tests/e2e/pg2yt/yt_static",
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
		Static:        true,
	}
	dst := &yt_provider.YtDestinationWrapper{Model: dstModel}
	dst.WithDefaults()

	transfer := helpers.MakeTransfer("upload_pg_yt_static", src, dst, abstract.TransferTypeSnapshotOnly)

	tablePath := ypath.Path("//home/cdc/tests/e2e/pg2yt/yt_static/test_table")

	t.Run("upload_without_cleanup", func(t *testing.T) {
		tables := []abstract.TableDescription{{Name: "test_table", Schema: "public"}}
		snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewStatefulFakeClient(), "test-operation1", transfer, helpers.EmptyRegistry())
		require.NoError(t, snapshotLoader.UploadTables(ctx, tables, true))
		table, err := ytEnv.YT.ReadTable(ctx, tablePath, nil)
		require.NoError(t, err)
		defer func(table yt.TableReader) {
			err := table.Close()
			require.NoError(t, err)
		}(table)
		for id := 1; id <= 100; id++ {
			require.Truef(t, table.Next(), "no row for id %v", id)
			var row testTableRow
			require.NoErrorf(t, table.Scan(&row), "unable to scan row for id %v", id)
			require.Equal(t, id, row.ID)
		}
		require.False(t, table.Next())
	})

	t.Run("upload_with_disabled_cleanup", func(t *testing.T) {
		connPool, err := postgres.MakeConnPoolFromSrc(src, lgr)
		require.NoError(t, err)
		_, err = connPool.Exec(ctx, `
INSERT INTO test_table
SELECT id
FROM generate_series(101, 200) AS t(id);
`)
		require.NoError(t, err)
		dstModel.Cleanup = model.DisabledCleanup
		tables := []abstract.TableDescription{{Name: "test_table", Schema: "public", Filter: "id >= 101 AND id <= 200"}}
		snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewStatefulFakeClient(), "test-operation2", transfer, helpers.EmptyRegistry())
		require.NoError(t, snapshotLoader.UploadTables(ctx, tables, true))
		table, err := ytEnv.YT.ReadTable(ctx, tablePath, nil)
		require.NoError(t, err)
		defer func(table yt.TableReader) {
			err := table.Close()
			require.NoError(t, err)
		}(table)
		for id := 1; id <= 200; id++ {
			require.Truef(t, table.Next(), "no row for id %v", id)
			var row testTableRow
			require.NoErrorf(t, table.Scan(&row), "unable to scan row for id %v", id)
			require.Equal(t, id, row.ID)
		}
		require.False(t, table.Next())
	})

	t.Run("upload_with_cleanup_drop", func(t *testing.T) {
		connPool, err := postgres.MakeConnPoolFromSrc(src, lgr)
		require.NoError(t, err)
		_, err = connPool.Exec(ctx, `
DELETE FROM test_table
WHERE id >= 101 AND id <= 200;
`)
		require.NoError(t, err)
		dstModel.Cleanup = model.Drop
		tables := []abstract.TableDescription{{Name: "test_table", Schema: "public"}}
		snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewStatefulFakeClient(), "test-operation3", transfer, helpers.EmptyRegistry())
		require.NoError(t, snapshotLoader.UploadTables(ctx, tables, true))
		table, err := ytEnv.YT.ReadTable(ctx, tablePath, nil)
		require.NoError(t, err)
		defer func(table yt.TableReader) {
			err := table.Close()
			require.NoError(t, err)
		}(table)
		for id := 1; id <= 100; id++ {
			require.Truef(t, table.Next(), "no row for id %v", id)
			var row testTableRow
			require.NoErrorf(t, table.Scan(&row), "unable to scan row for id %v", id)
			require.Equal(t, id, row.ID)
		}
		require.False(t, table.Next())
	})

	t.Run("upload_with_old_type_system_ver", func(t *testing.T) {
		transferWithOldVer := transfer
		transferWithOldVer.TypeSystemVersion = 1
		tables := []abstract.TableDescription{{Name: "test_timestamp", Schema: "public"}}
		snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewStatefulFakeClient(), "test-operation1", transferWithOldVer, helpers.EmptyRegistry())
		require.NoError(t, snapshotLoader.UploadTables(ctx, tables, true))
		table, err := ytEnv.YT.ReadTable(ctx, ypath.Path("//home/cdc/tests/e2e/pg2yt/yt_static/test_timestamp"), nil)
		require.NoError(t, err)
		defer func(table yt.TableReader) {
			err := table.Close()
			require.NoError(t, err)
		}(table)
		for id := 1; id <= 2; id++ {
			require.Truef(t, table.Next(), "no row for id %v", id)
			var row testTableRow
			require.NoErrorf(t, table.Scan(&row), "unable to scan row for id %v", id)
			require.Equal(t, id, row.ID)
		}
		require.False(t, table.Next())
	})

	t.Run("upload_with_lastest_type_system_ver", func(t *testing.T) {
		transferWithLatestVer := transfer
		transferWithLatestVer.TypeSystemVersion = model.LatestVersion
		tables := []abstract.TableDescription{{Name: "test_timestamp2", Schema: "public"}}
		snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewStatefulFakeClient(), "test-operation1", transferWithLatestVer, helpers.EmptyRegistry())
		require.NoError(t, snapshotLoader.UploadTables(ctx, tables, true))
		table, err := ytEnv.YT.ReadTable(ctx, ypath.Path("//home/cdc/tests/e2e/pg2yt/yt_static/test_timestamp2"), nil)
		require.NoError(t, err)
		defer func(table yt.TableReader) {
			err := table.Close()
			require.NoError(t, err)
		}(table)
		for id := 1; id <= 2; id++ {
			require.Truef(t, table.Next(), "no row for id %v", id)
			var row testTableRow
			require.NoErrorf(t, table.Scan(&row), "unable to scan row for id %v", id)
			require.Equal(t, id, row.ID)
		}
		require.False(t, table.Next())
	})
}
