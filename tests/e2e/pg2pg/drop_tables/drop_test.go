package snapshot

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	srcAll          = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix("SRC_"))
	srcFilter       = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix("SRC_"), pgrecipe.WithDBTables("public.ids_1", "public.ids_2"))
	srcNoViewAll    = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump_1"), pgrecipe.WithPrefix("NOVIEW_SRC_"))
	srcNoViewFilter = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump_1"), pgrecipe.WithPrefix("NOVIEW_SRC_"), pgrecipe.WithDBTables("public.items_1", "public.ids_1"))

	dstAllR          = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix("FULL_"))
	dstFilterR       = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix("FILTER_"))
	dstAllSR         = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix("FULL_S_"))
	dstNoViewAllR    = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix("NOVIEW_FULL_"))
	dstNoViewFilterR = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix("NOVIEW_FILTER_"))
	dstSelectiveR    = *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix("FULL_SELECTIVE_"))
)

const (
	existsT1Query = `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ids_1');`
	existsT2Query = `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ids_2');`
	existsT3Query = `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'items_2');`
	existsV1Query = `SELECT EXISTS(SELECT 1 FROM information_schema.views WHERE table_schema = 'public' AND table_name = 'spb_items_1_2020');`
	existsS1Query = `SELECT EXISTS(SELECT 1 FROM information_schema.sequences WHERE sequence_schema = 'public' AND sequence_name = 'ids_1_seq');`
	existsS2Query = `SELECT EXISTS(SELECT 1 FROM information_schema.sequences WHERE sequence_schema = 'public' AND sequence_name = 'items_1_seq');`
	existsS3Query = `SELECT EXISTS(SELECT 1 FROM information_schema.sequences WHERE sequence_schema = 'public' AND sequence_name = 'ids_2_seq');`
	existsS4Query = `SELECT EXISTS(SELECT 1 FROM information_schema.sequences WHERE sequence_schema = 'public' AND sequence_name = 'items_2_seq');`
)

func init() {
	_ = os.Setenv("YC", "1")
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source all", Port: srcAll.Port},
			helpers.LabeledPort{Label: "PG source noview", Port: srcNoViewAll.Port},
			helpers.LabeledPort{Label: "PG target all", Port: dstAllR.Port},
			helpers.LabeledPort{Label: "PG target filter", Port: dstFilterR.Port},
			helpers.LabeledPort{Label: "PG target filter snapshot", Port: dstAllSR.Port},
			helpers.LabeledPort{Label: "PG target noview", Port: dstNoViewAllR.Port},
			helpers.LabeledPort{Label: "PG target noview filter", Port: dstNoViewFilterR.Port},
			helpers.LabeledPort{Label: "PG target selective", Port: dstSelectiveR.Port},
		))
	}()

	srcAll.WithDefaults()
	srcFilter.WithDefaults()
	srcNoViewAll.WithDefaults()
	srcNoViewFilter.WithDefaults()
	dstAllR.WithDefaults()
	dstFilterR.WithDefaults()
	dstAllSR.WithDefaults()
	dstNoViewAllR.WithDefaults()
	dstNoViewFilterR.WithDefaults()
	dstSelectiveR.WithDefaults()

	t.Run("DROP cleanup policy test", func(t *testing.T) {
		t.Run("Drop all tables", DropAll)
		t.Run("Drop filtered tables", DropFilter)
		t.Run("Drop all tables in snapshot", DropAllSnapshotOnly)
		t.Run("Drop all tables with no VIEW at source", DropNoViewAll)
		t.Run("Drop filtered tables with no VIEW at source", DropNoViewFilter)
		t.Run("Drop selective tables with dependent VIEW", DropSelective)
	})
}

func DropAll(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &srcAll, &dstAllR, abstract.TransferTypeSnapshotAndIncrement)
	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.CleanupSinker(tables)
	require.NoError(t, err)

	conn, err := postgres.MakeConnPoolFromDst(&dstAllR, logger.Log)
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var exists bool
	require.NoError(t, conn.QueryRow(ctx, existsS1Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS2Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS3Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS4Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT1Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT2Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT3Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsV1Query).Scan(&exists))
	require.False(t, exists)
}

func DropFilter(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &srcFilter, &dstFilterR, abstract.TransferTypeSnapshotAndIncrement)
	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.CleanupSinker(tables)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot drop table ids_1 because other objects depend on it")

	conn, err := postgres.MakeConnPoolFromDst(&dstFilterR, logger.Log)
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var exists bool
	require.NoError(t, conn.QueryRow(ctx, existsS1Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS2Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS3Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS4Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT1Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT2Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT3Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsV1Query).Scan(&exists))
	require.True(t, exists)
}

func DropAllSnapshotOnly(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &srcAll, &dstAllSR, abstract.TransferTypeSnapshotOnly)
	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.CleanupSinker(tables)
	require.NoError(t, err)

	conn, err := postgres.MakeConnPoolFromDst(&dstAllSR, logger.Log)
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var exists bool
	require.NoError(t, conn.QueryRow(ctx, existsS1Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS2Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS3Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS4Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT1Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT2Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT3Query).Scan(&exists))
	require.False(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsV1Query).Scan(&exists))
	require.False(t, exists)
}

func DropNoViewAll(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &srcNoViewAll, &dstNoViewAllR, abstract.TransferTypeSnapshotAndIncrement)
	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	// must not drop VIEW in target when it is absent in source
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.CleanupSinker(tables)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed dependent VIEWs check")

	conn, err := postgres.MakeConnPoolFromDst(&dstNoViewAllR, logger.Log)
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var exists bool
	require.NoError(t, conn.QueryRow(ctx, existsS1Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS2Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS3Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS4Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT1Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT2Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT3Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsV1Query).Scan(&exists))
	require.True(t, exists)
}

func DropNoViewFilter(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &srcNoViewFilter, &dstNoViewFilterR, abstract.TransferTypeSnapshotAndIncrement)
	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)
	logger.Log.Infof("got tables: %v", tables)

	// must not drop VIEW in target when it is absent in source
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.CleanupSinker(tables)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed dependent VIEWs check")

	conn, err := postgres.MakeConnPoolFromDst(&dstNoViewFilterR, logger.Log)
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var exists bool
	require.NoError(t, conn.QueryRow(ctx, existsS1Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS2Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS3Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsS4Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT1Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT2Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsT3Query).Scan(&exists))
	require.True(t, exists)
	require.NoError(t, conn.QueryRow(ctx, existsV1Query).Scan(&exists))
	require.True(t, exists)
}

func DropSelective(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &srcAll, &dstSelectiveR, abstract.TransferTypeSnapshotAndIncrement)
	tables := abstract.TableMap{
		abstract.TableID{Namespace: "public", Name: "items_1"}: *new(abstract.TableInfo),
	}
	logger.Log.Infof("got tables: %v", tables)

	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err := snapshotLoader.CleanupSinker(tables)
	require.NoError(t, err)

	dstStorage, err := postgres.NewStorage(dstSelectiveR.ToStorageParams())
	require.NoError(t, err)
	defer dstStorage.Close()

	tablesAfterCleanup, err := model.FilteredTableList(dstStorage, transfer)
	require.NoError(t, err)

	_, items1Exists := tablesAfterCleanup[abstract.TableID{Namespace: "public", Name: "items_1"}]
	require.False(t, items1Exists)

	_, items1ViewExists := tablesAfterCleanup[abstract.TableID{Namespace: "public", Name: "spb_items_1_2020"}]
	require.False(t, items1ViewExists)

	_, items2Exists := tablesAfterCleanup[abstract.TableID{Namespace: "public", Name: "items_2"}]
	require.True(t, items2Exists)
}
