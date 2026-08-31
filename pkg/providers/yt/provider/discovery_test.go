package provider

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/cypressmeta"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

func buildSchema(schema []yt_provider.ColumnSchema) []map[string]string {
	res := make([]map[string]string, len(schema))
	for idx, col := range schema {
		res[idx] = map[string]string{
			"name": col.Name,
			"type": string(col.YTType),
		}
	}

	return res
}

func TestTablesDiscovery(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()

	ctx := context.Background()

	rootPath := ypath.Path("//home/cdc/junk/TestTablesDiscovery")
	_, err := env.YT.CreateNode(ctx, rootPath, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	defer func() {
		err := env.YT.RemoveNode(ctx, rootPath, &yt.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	}()

	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_1")))
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_2")))
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_3")))
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_4")))
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_5")))
	_, err = env.YT.CreateNode(ctx, rootPath.Child("some_dir"), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("some_dir").Child("sample_table_1")))
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("some_dir").Child("sample_table_2")))
	_, err = env.YT.CreateNode(ctx, rootPath.Child("some_dir").Child("sample_non_table_obj"), yt.NodeFile, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	t.Run("all_tables", func(t *testing.T) {
		cfg := &yt_provider.YtSource{
			Cluster: os.Getenv("YT_PROXY"),
			YtProxy: os.Getenv("YT_PROXY"),
			Paths:   []string{rootPath.String()},
			YtToken: os.Getenv("YT_TOKEN"),
		}

		src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg, nil)
		require.NoError(t, err)

		require.NoError(t, src.BeginSnapshot(ctx))
		defer func() { _ = src.EndSnapshot(ctx) }()

		tables, err := src.TableList(nil)
		require.NoError(t, err)
		require.Len(t, tables, 7)
	})
	t.Run("2_tables", func(t *testing.T) {
		cfg := &yt_provider.YtSource{
			Cluster: os.Getenv("YT_PROXY"),
			YtProxy: os.Getenv("YT_PROXY"),
			Paths:   []string{rootPath.Child("sample_table_2").String(), rootPath.Child("sample_table_5").String()},
			YtToken: os.Getenv("YT_TOKEN"),
		}

		src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg, nil)
		require.NoError(t, err)

		require.NoError(t, src.BeginSnapshot(ctx))
		defer func() { _ = src.EndSnapshot(ctx) }()

		tables, err := src.TableList(nil)
		require.NoError(t, err)
		require.Len(t, tables, 2)
	})
	t.Run("error_for_non_tables", func(t *testing.T) {
		cfg := &yt_provider.YtSource{
			Cluster: os.Getenv("YT_PROXY"),
			YtProxy: os.Getenv("YT_PROXY"),
			Paths:   []string{rootPath.Child("some_dir").Child("sample_non_table_obj").String()},
			YtToken: os.Getenv("YT_TOKEN"),
		}

		src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg, nil)
		require.NoError(t, err)

		require.NoError(t, src.BeginSnapshot(ctx))
		defer func() { _ = src.EndSnapshot(ctx) }()

		_, err = src.TableList(nil)
		require.Error(t, err)
	})
	t.Run("error_when_colliding_names", func(t *testing.T) {
		cfg := &yt_provider.YtSource{
			Cluster: os.Getenv("YT_PROXY"),
			YtProxy: os.Getenv("YT_PROXY"),
			Paths:   []string{rootPath.Child("sample_table_2").String(), rootPath.Child("some_dir").String()},
			YtToken: os.Getenv("YT_TOKEN"),
		}

		src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg, nil)
		require.NoError(t, err)

		require.NoError(t, src.BeginSnapshot(ctx))
		defer func() { _ = src.EndSnapshot(ctx) }()

		_, err = src.TableList(nil)
		require.ErrorContains(t, err, "collision")
	})
}

func createTestTable(env *yttest.Env, ctx context.Context, tablePath ypath.Path) error {
	_, err := env.YT.CreateNode(ctx, tablePath, yt.NodeTable, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"schema": buildSchema([]yt_provider.ColumnSchema{
				{
					Name:    "Column_1",
					YTType:  "int8",
					Primary: true,
				},
				{
					Name:    "Column_2",
					YTType:  "int8",
					Primary: false,
				},
			},
			),
		},
	})
	return err
}

func testYtSourceCfg(rootPath ypath.Path) *yt_provider.YtSource {
	return &yt_provider.YtSource{
		Cluster: os.Getenv("YT_PROXY"),
		YtProxy: os.Getenv("YT_PROXY"),
		Paths:   []string{rootPath.String()},
		YtToken: os.Getenv("YT_TOKEN"),
	}
}

// testRow mirrors the schema created by createTestTable. WriteTable infers the
// table schema from the first written value, and map[string]interface{} is not
// inferable, so rows must be typed.
type testRow struct {
	Column1 int64 `yson:"Column_1"`
	Column2 int64 `yson:"Column_2"`
}

func writeRows(t *testing.T, env *yttest.Env, tablePath ypath.Path, n int) {
	t.Helper()
	// WriteTable recreates the table (schema is inferred from the first row),
	// so the pre-created table must be dropped first.
	require.NoError(t, env.YT.RemoveNode(env.Ctx, tablePath, &yt.RemoveNodeOptions{Recursive: true}))
	w, err := yt.WriteTable(env.Ctx, env.YT, tablePath)
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		require.NoError(t, w.Write(testRow{Column1: int64(i), Column2: int64(i * 2)}))
	}
	require.NoError(t, w.Commit())
}

// TestUniqueTableIDsDedup covers P1#12: the same table listed twice under
// overlapping roots is deduplicated by OriginalPath (first occurrence wins),
// while two DIFFERENT tables with the same relative name still collide.
func TestUniqueTableIDsDedup(t *testing.T) {
	// Same table seen from an overlapping root pair: no error, first wins.
	nodes := cypressmeta.YtNodes{
		cypressmeta.NewYtNodeMeta("c", "//home/root", "/some_dir/t1", 1, 10, 0, yt.NodeTable, false, nil, nil),
		cypressmeta.NewYtNodeMeta("c", "//home/root/some_dir", "/t1", 1, 10, 0, yt.NodeTable, false, nil, nil),
	}
	res, err := uniqueTableIDs(nodes)
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, "some_dir/t1", res[0].Name)

	// Different tables with identical relative names must be rejected.
	other := cypressmeta.YtNodes{
		cypressmeta.NewYtNodeMeta("c", "//home/a", "/t", 1, 10, 0, yt.NodeTable, false, nil, nil),
		cypressmeta.NewYtNodeMeta("c", "//home/b", "/t", 1, 10, 0, yt.NodeTable, false, nil, nil),
	}
	_, err = uniqueTableIDs(other)
	require.ErrorContains(t, err, "collision")
}

// TestBeginSnapshotIdempotent covers P1#7-adjacent TX lifecycle: a repeated
// BeginSnapshot must not open a second TX, but BeginSnapshot after
// EndSnapshot must open a fresh one.
func TestBeginSnapshotIdempotent(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	ctx := context.Background()

	rootPath := ypath.Path("//home/cdc/junk/TestBeginSnapshotIdempotent")
	_, err := env.YT.CreateNode(ctx, rootPath, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	defer func() { _ = env.YT.RemoveNode(ctx, rootPath, &yt.RemoveNodeOptions{Recursive: true}) }()
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_1")))

	src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), testYtSourceCfg(rootPath), nil)
	require.NoError(t, err)

	require.NoError(t, src.BeginSnapshot(ctx))
	txID1 := src.txID
	require.NoError(t, src.BeginSnapshot(ctx))
	require.Equal(t, txID1, src.txID, "second BeginSnapshot must not open another TX")
	require.NoError(t, src.EndSnapshot(ctx))
	require.NoError(t, src.BeginSnapshot(ctx))
	require.NotEqual(t, txID1, src.txID, "BeginSnapshot after EndSnapshot must open a fresh TX")
}

// TestSetShardingContextAttachesTx covers the P0 fix: a secondary worker that
// only received the sharding context must attach to the snapshot TX and lock
// tables (NodeID populated) without calling BeginSnapshot.
func TestSetShardingContextAttachesTx(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	ctx := context.Background()

	rootPath := ypath.Path("//home/cdc/junk/TestSetShardingContextAttachesTx")
	_, err := env.YT.CreateNode(ctx, rootPath, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	defer func() { _ = env.YT.RemoveNode(ctx, rootPath, &yt.RemoveNodeOptions{Recursive: true}) }()
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_1")))

	cfg := testYtSourceCfg(rootPath)

	main, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg, nil)
	require.NoError(t, err)
	require.NoError(t, main.BeginSnapshot(ctx))
	defer func() { _ = main.EndSnapshot(ctx) }()
	shardedState, err := main.ShardingContext()
	require.NoError(t, err)

	secondary, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg, nil)
	require.NoError(t, err)
	require.NoError(t, secondary.SetShardingContext(shardedState))

	tables, err := secondary.listTables(ctx)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.NotNil(t, tables[0].NodeID, "listTables on secondary worker must lock tables through the attached TX")

	// Closing a secondary must not abort the shared master TX: the main
	// worker keeps reading through it until EndSnapshot.
	secondary.Close()
	_, err = main.tx.LockNode(ctx, tables[0].OriginalYPath(), yt.LockSnapshot, nil)
	require.NoError(t, err, "shared snapshot TX must survive secondary Close")

	// A worker that opened its own TX before SetShardingContext must drop the
	// local TX and switch ownership to the attached handle.
	lateSecondary, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg, nil)
	require.NoError(t, err)
	require.NoError(t, lateSecondary.BeginSnapshot(ctx))
	ownTx := lateSecondary.tx
	require.NoError(t, lateSecondary.SetShardingContext(shardedState))
	require.False(t, lateSecondary.txOwned)
	select {
	case <-ownTx.Finished():
	default:
		t.Fatal("locally-started TX must be aborted by SetShardingContext")
	}
	lateSecondary.Close()

	// A listing that raced ahead of SetShardingContext filled the caches
	// through the local TX; after the local TX is aborted the caches must be
	// dropped so the next listing goes through the attached snapshot TX.
	racedSecondary, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg, nil)
	require.NoError(t, err)
	defer racedSecondary.Close()
	require.NoError(t, racedSecondary.BeginSnapshot(ctx))
	_, err = racedSecondary.listTables(ctx)
	require.NoError(t, err)
	require.NotNil(t, racedSecondary.nodes)
	require.NoError(t, racedSecondary.SetShardingContext(shardedState))
	require.Nil(t, racedSecondary.nodes, "listing cache from the aborted local TX must be dropped")
	require.Nil(t, racedSecondary.partsMapping, "shard budget from the aborted local TX must be dropped")
}

// TestSetShardingContextEmptyTxID covers a missing TX id in the sharding
// state: the worker must fail fast instead of reading outside any snapshot.
func TestSetShardingContextEmptyTxID(t *testing.T) {
	s := &source{logger: logger.Log}
	state, err := yson.Marshal(&SnapshotState{TxID: yt.TxID{}})
	require.NoError(t, err)
	require.ErrorContains(t, s.SetShardingContext(state), "empty tx id")
}

// TestFilterByIncludesPreservesListingOrder covers the FTL reordering: for
// overlapping include roots the kept node is the first one in listing order,
// not the first matching include.
func TestFilterByIncludesPreservesListingOrder(t *testing.T) {
	s := &source{}
	nodes := cypressmeta.YtNodes{
		cypressmeta.NewYtNodeMeta("c", "//home/root", "/dir/t1", 1, 10, 0, yt.NodeTable, false, nil, nil),
		cypressmeta.NewYtNodeMeta("c", "//home/root/dir", "/t1", 1, 10, 0, yt.NodeTable, false, nil, nil),
	}
	res, err := s.filterByIncludes(context.Background(), nodes, []string{"//home/root/dir", "//home/root"})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, "dir/t1", res[0].Name, "listing order must win over include order")
}

// TestFilterByIncludesDisambiguatesCollision covers the collision-after-filter
// behaviour: a name collision that would fail on the full listing must not
// fire when includes select only one of the colliding tables.
func TestFilterByIncludesDisambiguatesCollision(t *testing.T) {
	s := &source{}
	nodes := cypressmeta.YtNodes{
		cypressmeta.NewYtNodeMeta("c", "//home/a", "/t", 1, 10, 0, yt.NodeTable, false, nil, nil),
		cypressmeta.NewYtNodeMeta("c", "//home/b", "/t", 1, 10, 0, yt.NodeTable, false, nil, nil),
	}
	filtered, err := s.filterByIncludes(context.Background(), nodes, []string{"//home/a/t"})
	require.NoError(t, err)
	unique, err := uniqueTableIDs(filtered)
	require.NoError(t, err)
	require.Len(t, unique, 1)
	require.Equal(t, "t", unique[0].Name)
}

// TestFilterByIncludesSkipsEmptyDir: an include object that exists but has no
// tables under it (empty directory, plain file) must not fail the snapshot —
// parity with the legacy filter, which simply contributed no tables for such
// includes. A nonexistent include still fails the listing.
func TestFilterByIncludesSkipsEmptyDir(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	ctx := context.Background()

	rootPath := ypath.Path("//home/cdc/junk/TestFilterByIncludesSkipsEmptyDir")
	_, err := env.YT.CreateNode(ctx, rootPath, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	defer func() { _ = env.YT.RemoveNode(ctx, rootPath, &yt.RemoveNodeOptions{Recursive: true}) }()
	emptyDir := rootPath.Child("empty_dir")
	_, err = env.YT.CreateNode(ctx, emptyDir, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_1")))

	cfg := testYtSourceCfg(rootPath)
	src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg, nil)
	require.NoError(t, err)
	defer src.Close()

	transfer := &model.Transfer{DataObjects: &model.DataObjects{IncludeObjects: []string{
		emptyDir.String(),
		rootPath.Child("sample_table_1").String(),
	}}}
	maps, err := src.FilteredTableList(transfer)
	require.NoError(t, err, "an existing empty include directory must not fail the snapshot")
	require.Len(t, maps, 1)

	// A nonexistent include fails at the listing stage, as in the trunk flow.
	transferNonexistent := &model.Transfer{DataObjects: &model.DataObjects{IncludeObjects: []string{
		rootPath.Child("no_such_object").String(),
	}}}
	_, err = src.FilteredTableList(transferNonexistent)
	require.Error(t, err)
}

// TestFilteredTableListColumnProjection covers P1#3/#4: FTL listing locks
// tables inside the snapshot TX and loads schemas with the column projection.
func TestFilteredTableListColumnProjection(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	ctx := context.Background()

	rootPath := ypath.Path("//home/cdc/junk/TestFilteredTableListColumnProjection")
	_, err := env.YT.CreateNode(ctx, rootPath, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	defer func() { _ = env.YT.RemoveNode(ctx, rootPath, &yt.RemoveNodeOptions{Recursive: true}) }()
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("sample_table_1")))

	include := rootPath.Child("sample_table_1").String() + "{Column_1}"
	src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), testYtSourceCfg(rootPath), []string{include})
	require.NoError(t, err)

	transfer := &model.Transfer{DataObjects: &model.DataObjects{IncludeObjects: []string{include}}}
	maps, err := src.FilteredTableList(transfer)
	require.NoError(t, err)
	require.Len(t, maps, 1)
	var schema *abstract.TableSchema
	for _, info := range maps {
		schema = info.Schema
	}
	require.NotNil(t, schema)
	cols := schema.Columns()
	require.Len(t, cols, 1)
	require.Equal(t, "Column_1", cols[0].ColumnName)
}

// TestShardTableSkipsEmptyTables covers P1#2: empty tables produce no parts.
func TestShardTableSkipsEmptyTables(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	ctx := context.Background()

	rootPath := ypath.Path("//home/cdc/junk/TestShardTableSkipsEmptyTables")
	_, err := env.YT.CreateNode(ctx, rootPath, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	defer func() { _ = env.YT.RemoveNode(ctx, rootPath, &yt.RemoveNodeOptions{Recursive: true}) }()
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("empty_table"))) // 0 rows

	src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), testYtSourceCfg(rootPath), nil)
	require.NoError(t, err)
	require.NoError(t, src.BeginSnapshot(ctx))
	defer func() { _ = src.EndSnapshot(ctx) }()

	parts, err := src.ShardTable(ctx, abstract.TableDescription{Name: "empty_table", Schema: ""})
	require.NoError(t, err)
	require.Empty(t, parts)
}

// TestShardTableGlobalBudget checks the source-level plumbing of the shard
// budget: the mapping is computed once per snapshot and reused across
// repeated ShardTable calls. The global-budget math itself is covered by
// TestComputePartsMappingGlobalBudget (a pure unit test with synthetic
// weights — with 60000-row tables the MinShardSize cap makes the parts sum
// trivially small).
func TestShardTableGlobalBudget(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	ctx := context.Background()

	rootPath := ypath.Path("//home/cdc/junk/TestShardTableGlobalBudget")
	_, err := env.YT.CreateNode(ctx, rootPath, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	defer func() { _ = env.YT.RemoveNode(ctx, rootPath, &yt.RemoveNodeOptions{Recursive: true}) }()
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("table_a")))
	require.NoError(t, createTestTable(env, ctx, rootPath.Child("table_b")))
	writeRows(t, env, rootPath.Child("table_a"), 60000)
	writeRows(t, env, rootPath.Child("table_b"), 60000)

	cfg := testYtSourceCfg(rootPath)
	cfg.DesiredPartSizeBytes = 1024
	src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), cfg, nil)
	require.NoError(t, err)
	require.NoError(t, src.BeginSnapshot(ctx))
	defer func() { _ = src.EndSnapshot(ctx) }()

	partsA, err := src.ShardTable(ctx, abstract.TableDescription{Name: "table_a", Schema: ""})
	require.NoError(t, err)
	require.NotEmpty(t, partsA)
	partsARepeat, err := src.ShardTable(ctx, abstract.TableDescription{Name: "table_a", Schema: ""})
	require.NoError(t, err)
	require.Equal(t, partsA, partsARepeat, "shard budget must be computed once and reused")
	partsB, err := src.ShardTable(ctx, abstract.TableDescription{Name: "table_b", Schema: ""})
	require.NoError(t, err)
	require.NotEmpty(t, partsB)
	require.LessOrEqual(t, len(partsA)+len(partsB), 1024)
}

// bigRow carries a ~4KiB payload so a few hundred rows exceed PushBatchSize.
type bigRow struct {
	Column1 int64  `yson:"Column_1"`
	Column2 string `yson:"Column_2"`
}

// TestPushLoopEmitsSynchronize: after synchronizeFlushBytes of pushed data
// the source pushes a Synchronize item for the current part between data
// batches, so sinks flush partially loaded data — the provider-internal
// replacement for the legacy abstract2 pusher window flush.
func TestPushLoopEmitsSynchronize(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()
	ctx := context.Background()

	rootPath := ypath.Path("//home/cdc/junk/TestPushLoopEmitsSynchronize")
	_, err := env.YT.CreateNode(ctx, rootPath, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)
	defer func() { _ = env.YT.RemoveNode(ctx, rootPath, &yt.RemoveNodeOptions{Recursive: true}) }()

	w, err := yt.WriteTable(ctx, env.YT, rootPath.Child("sample_table_1"))
	require.NoError(t, err)
	for i := 0; i < 600; i++ {
		require.NoError(t, w.Write(bigRow{Column1: int64(i), Column2: strings.Repeat("x", 4096)}))
	}
	require.NoError(t, w.Commit())

	oldBudget := synchronizeFlushBytes
	synchronizeFlushBytes = 1
	defer func() { synchronizeFlushBytes = oldBudget }()

	src, err := NewSource(logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()), testYtSourceCfg(rootPath), nil)
	require.NoError(t, err)
	defer src.Close()
	require.NoError(t, src.BeginSnapshot(ctx))
	defer func() { _ = src.EndSnapshot(ctx) }()

	var pushes [][]abstract.ChangeItem
	err = src.LoadTable(ctx, abstract.TableDescription{Name: "sample_table_1", Schema: ""}, func(items []abstract.ChangeItem) error {
		pushes = append(pushes, items)
		return nil
	})
	require.NoError(t, err)

	// Pattern: data batch, Synchronize, data batch, ... — the final batch is
	// never followed by a flush (DoneTableLoad does that).
	require.GreaterOrEqual(t, len(pushes), 3)
	require.Equal(t, 1, len(pushes)%2, "pushes must end with a data batch")
	totalRows := 0
	for i, p := range pushes {
		if i%2 == 0 {
			for _, item := range p {
				require.Equal(t, abstract.InsertKind, item.Kind)
			}
			totalRows += len(p)
		} else {
			require.Len(t, p, 1)
			require.Equal(t, abstract.SynchronizeKind, p[0].Kind)
			require.Equal(t, "sample_table_1", p[0].Table)
			require.Equal(t, "", p[0].Schema)
			require.Equal(t, "", p[0].PartID)
		}
	}
	require.Equal(t, 600, totalRows)
}
