package merge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/cleanup"
	yt2 "github.com/transferria/transferria/pkg/providers/yt"
	ytclient "github.com/transferria/transferria/pkg/providers/yt/client"
	ytsink "github.com/transferria/transferria/pkg/providers/yt/sink"
	"github.com/transferria/transferria/pkg/randutil"
	"github.com/transferria/transferria/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type testObject struct {
	Key   string `yson:"key,key"`
	Value string `yson:"value"`
}

type testAttrs struct {
	EnableDynamicStoreRead bool `yson:"enable_dynamic_store_read"`
}

func testYT(t *testing.T, testName string, test func(t *testing.T, ctx context.Context, client yt.Client, path ypath.Path, logger log.Logger)) {
	// To run test locally set YT_PROXY and YT_TOKEN
	config := new(yt.Config)
	client, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, config)
	require.NoError(t, err)
	defer client.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testDir := randutil.GenerateAlphanumericString(10)

	path := yt2.SafeChild("//home/cdc/test", testName, testDir)
	logger.Log.Infof("test dir: %v", path)
	_, err = client.CreateNode(ctx, path, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	test(t, ctx, client, path, logger.Log)

	err = client.RemoveNode(ctx, path, &yt.RemoveNodeOptions{Recursive: true})
	require.NoError(t, err)
}

func TestMergeBasic(t *testing.T) {
	testYT(t, "sink_yt_merge/basic", func(t *testing.T, ctx context.Context, client yt.Client, path ypath.Path, logger log.Logger) {
		require.NoError(t, createTables(ctx, client, logger, false, map[ypath.Path][]testObject{
			yt2.SafeChild(path, "basic_table_part_0"): {
				{Key: "4", Value: "new"},
				{Key: "7"},
			},
			yt2.SafeChild(path, "basic_table_part_1"): {
				{Key: "5"},
				{Key: "5"},
				{Key: "8"},
				{Key: "9"},
			},
			yt2.SafeChild(path, "basic_table_part_2"): {
				{Key: "6"},
				{Key: "8"},
				{Key: "9"},
			},
		}))
		require.NoError(t, createTable(ctx, client, logger, yt2.SafeChild(path, "basic_table"), true, []testObject{
			{Key: "1"},
			{Key: "2"},
			{Key: "3"},
			{Key: "4", Value: "old"},
		}))
		mergeAndCheck(t, ctx, client, logger, path, "", "basic_table_part", "_part", map[string][]testObject{
			"basic_table": {
				{Key: "1"},
				{Key: "2"},
				{Key: "3"},
				{Key: "4", Value: "new"},
				{Key: "5"},
				{Key: "6"},
				{Key: "7"},
				{Key: "8"},
				{Key: "9"},
			},
		}, false)
		nodes, err := yt2.ListNodesWithAttrs(ctx, client, path, "", false)
		require.NoError(t, err)
		require.Equal(t, 1, len(nodes))
		require.Equal(t, "basic_table", nodes[0].Name)
		require.True(t, nodes[0].Attrs.Dynamic)
		require.Equal(t, yt.TabletMounted, nodes[0].Attrs.TabletState)
	})
}

func TestMergeBasicCleanup(t *testing.T) {
	testYT(t, "sink_yt_merge/basic_cleanup", func(t *testing.T, ctx context.Context, client yt.Client, path ypath.Path, logger log.Logger) {
		require.NoError(t, createTables(ctx, client, logger, false, map[ypath.Path][]testObject{
			yt2.SafeChild(path, "basic_table_part_0"): {
				{Key: "4", Value: "new"},
				{Key: "7"},
			},
			yt2.SafeChild(path, "basic_table_part_1"): {
				{Key: "5"},
				{Key: "5"},
				{Key: "8"},
				{Key: "9"},
			},
			yt2.SafeChild(path, "basic_table_part_2"): {
				{Key: "6"},
				{Key: "8"},
				{Key: "9"},
			},
		}))
		require.NoError(t, createTable(ctx, client, logger, yt2.SafeChild(path, "basic_table"), true, []testObject{
			{Key: "1"},
			{Key: "2"},
			{Key: "3"},
			{Key: "4", Value: "old"},
		}))
		mergeAndCheck(t, ctx, client, logger, path, yt2.SafeChild(path, "basic_table"), "basic_table_part", "_part", map[string][]testObject{
			"basic_table": {
				{Key: "4", Value: "new"},
				{Key: "5"},
				{Key: "6"},
				{Key: "7"},
				{Key: "8"},
				{Key: "9"},
			},
		}, false)
		nodes, err := yt2.ListNodesWithAttrs(ctx, client, path, "", false)
		require.NoError(t, err)
		require.Equal(t, 1, len(nodes))
		require.Equal(t, "basic_table", nodes[0].Name)
		require.True(t, nodes[0].Attrs.Dynamic)
		require.Equal(t, yt.TabletMounted, nodes[0].Attrs.TabletState)
	})
}

func TestMergeSingle(t *testing.T) {
	testYT(t, "sink_yt_merge/single", func(t *testing.T, ctx context.Context, client yt.Client, path ypath.Path, logger log.Logger) {
		require.NoError(t, createTables(ctx, client, logger, false, map[ypath.Path][]testObject{
			yt2.SafeChild(path, "single_table_part_0"): {
				{Key: "1", Value: "new"},
				{Key: "2"},
				{Key: "3"},
				{Key: "3"},
			},
		}))
		require.NoError(t, createTable(ctx, client, logger, yt2.SafeChild(path, "single_table"), true, []testObject{
			{Key: "1", Value: "old"},
			{Key: "4"},
			{Key: "5"},
			{Key: "6"},
		}))
		mergeAndCheck(t, ctx, client, logger, path, "", "single_table_part", "_part", map[string][]testObject{
			"single_table": {
				{Key: "1", Value: "new"},
				{Key: "2"},
				{Key: "3"},
				{Key: "4"},
				{Key: "5"},
				{Key: "6"},
			},
		}, false)
		nodes, err := yt2.ListNodesWithAttrs(ctx, client, path, "", false)
		require.NoError(t, err)
		require.Equal(t, 1, len(nodes))
		require.Equal(t, "single_table", nodes[0].Name)
		require.True(t, nodes[0].Attrs.Dynamic)
		require.Equal(t, yt.TabletMounted, nodes[0].Attrs.TabletState)
	})
}

func TestMergeRotated(t *testing.T) {
	testYT(t, "sink_yt_merge/rotated", func(t *testing.T, ctx context.Context, client yt.Client, path ypath.Path, logger log.Logger) {
		require.NoError(t, createTables(ctx, client, logger, false, map[ypath.Path][]testObject{
			yt2.SafeChild(path, "rotated_table_part_0/2022-07-11"): {
				{Key: "0"},
			},
			yt2.SafeChild(path, "rotated_table_part_0/2022-07-12"): {
				{Key: "0", Value: "new"},
				{Key: "1"},
				{Key: "1"},
			},
			yt2.SafeChild(path, "rotated_table_part_1/2022-07-12"): {
				{Key: "2"},
			},
			yt2.SafeChild(path, "rotated_table_part_2/2022-07-12"): {
				{Key: "2"},
				{Key: "3"},
			},
			yt2.SafeChild(path, "rotated_table_part_0/2022-07-13"): {
				{Key: "4"},
			},
			yt2.SafeChild(path, "rotated_table_part_1/2022-07-13"): {
				{Key: "5"},
			},
			yt2.SafeChild(path, "rotated_table_part_2/2022-07-13"): {
				{Key: "6"},
			},
			yt2.SafeChild(path, "rotated_table_part_0/2022-07-14"): {
				{Key: "7"},
			},
			yt2.SafeChild(path, "rotated_table_part_1/2022-07-14"): {
				{Key: "8"},
			},
			yt2.SafeChild(path, "rotated_table_part_2/2022-07-14"): {
				{Key: "9"},
			},
		}))
		require.NoError(t, createTables(ctx, client, logger, true, map[ypath.Path][]testObject{
			yt2.SafeChild(path, "rotated_table/2022-07-11"): {},
			yt2.SafeChild(path, "rotated_table/2022-07-12"): {
				{Key: "0", Value: "old"},
			},
			yt2.SafeChild(path, "rotated_table/2022-07-13"): {
				{Key: "0"},
			},
			yt2.SafeChild(path, "rotated_table/2022-07-14"): {
				{Key: "0"},
			},
			yt2.SafeChild(path, "rotated_table/2022-07-15"): {
				{Key: "10"},
				{Key: "11"},
				{Key: "12"},
			},
		}))
		mergeAndCheck(t, ctx, client, logger, path, "", "rotated_table_part", "_part", map[string][]testObject{
			"rotated_table/2022-07-11": {
				{Key: "0"},
			},
			"rotated_table/2022-07-12": {
				{Key: "0", Value: "new"},
				{Key: "1"},
				{Key: "2"},
				{Key: "3"},
			},
			"rotated_table/2022-07-13": {
				{Key: "0"},
				{Key: "4"},
				{Key: "5"},
				{Key: "6"},
			},
			"rotated_table/2022-07-14": {
				{Key: "0"},
				{Key: "7"},
				{Key: "8"},
				{Key: "9"},
			},
		}, true)
		check(t, ctx, client, logger, yt2.SafeChild(path, "rotated_table/2022-07-15"),
			[]testObject{
				{Key: "10"},
				{Key: "11"},
				{Key: "12"},
			},
			testAttrs{EnableDynamicStoreRead: false}, // cause this table is not modified my merge
		)

		nodes, err := yt2.ListNodesWithAttrs(ctx, client, path, "", false)
		require.NoError(t, err)
		require.Equal(t, 1, len(nodes))
		require.Equal(t, "rotated_table", nodes[0].Name)
		require.Equal(t, yt.NodeMap, nodes[0].Attrs.Type)

		nodes, err = yt2.ListNodesWithAttrs(ctx, client, yt2.SafeChild(path, "rotated_table"), "", false)
		require.NoError(t, err)
		require.Equal(t, 5, len(nodes))
		for _, node := range nodes {
			require.True(t, node.Attrs.Dynamic)
			require.Equal(t, yt.TabletMounted, node.Attrs.TabletState)
		}
	})
}

func TestMergeRotatedCleanup(t *testing.T) {
	testYT(t, "sink_yt_merge/rotated_cleanup", func(t *testing.T, ctx context.Context, client yt.Client, path ypath.Path, logger log.Logger) {
		require.NoError(t, createTables(ctx, client, logger, false, map[ypath.Path][]testObject{
			yt2.SafeChild(path, "rotated_table_part_0/2022-07-11"): {
				{Key: "0"},
			},
			yt2.SafeChild(path, "rotated_table_part_0/2022-07-12"): {
				{Key: "0", Value: "new"},
				{Key: "1"},
				{Key: "1"},
			},
			yt2.SafeChild(path, "rotated_table_part_1/2022-07-12"): {
				{Key: "2"},
			},
			yt2.SafeChild(path, "rotated_table_part_2/2022-07-12"): {
				{Key: "2"},
				{Key: "3"},
			},
			yt2.SafeChild(path, "rotated_table_part_0/2022-07-13"): {
				{Key: "4"},
			},
			yt2.SafeChild(path, "rotated_table_part_1/2022-07-13"): {
				{Key: "5"},
			},
			yt2.SafeChild(path, "rotated_table_part_2/2022-07-13"): {
				{Key: "6"},
			},
			yt2.SafeChild(path, "rotated_table_part_0/2022-07-14"): {
				{Key: "7"},
			},
			yt2.SafeChild(path, "rotated_table_part_1/2022-07-14"): {
				{Key: "8"},
			},
			yt2.SafeChild(path, "rotated_table_part_2/2022-07-14"): {
				{Key: "9"},
			},
		}))
		require.NoError(t, createTables(ctx, client, logger, true, map[ypath.Path][]testObject{
			yt2.SafeChild(path, "rotated_table/2022-07-11"): {},
			yt2.SafeChild(path, "rotated_table/2022-07-12"): {
				{Key: "0", Value: "old"},
			},
			yt2.SafeChild(path, "rotated_table/2022-07-13"): {
				{Key: "0"},
			},
			yt2.SafeChild(path, "rotated_table/2022-07-14"): {
				{Key: "0"},
			},
			yt2.SafeChild(path, "rotated_table/2022-07-15"): {
				{Key: "10"},
				{Key: "11"},
				{Key: "12"},
			},
		}))
		mergeAndCheck(t, ctx, client, logger, path, yt2.SafeChild(path, "rotated_table"), "rotated_table_part", "_part", map[string][]testObject{
			"rotated_table/2022-07-11": {
				{Key: "0"},
			},
			"rotated_table/2022-07-12": {
				{Key: "0", Value: "new"},
				{Key: "1"},
				{Key: "2"},
				{Key: "3"},
			},
			"rotated_table/2022-07-13": {
				{Key: "4"},
				{Key: "5"},
				{Key: "6"},
			},
			"rotated_table/2022-07-14": {
				{Key: "7"},
				{Key: "8"},
				{Key: "9"},
			},
		}, true)

		nodes, err := yt2.ListNodesWithAttrs(ctx, client, path, "", false)
		require.NoError(t, err)
		require.Equal(t, 1, len(nodes))
		require.Equal(t, "rotated_table", nodes[0].Name)
		require.Equal(t, yt.NodeMap, nodes[0].Attrs.Type)

		nodes, err = yt2.ListNodesWithAttrs(ctx, client, yt2.SafeChild(path, "rotated_table"), "", false)
		require.NoError(t, err)
		require.Equal(t, 4, len(nodes))
		for _, node := range nodes {
			require.True(t, node.Attrs.Dynamic)
			require.Equal(t, yt.TabletMounted, node.Attrs.TabletState)
		}
	})
}

func createTable(ctx context.Context, client yt.Client, logger log.Logger, path ypath.Path, dynamic bool, data []testObject) error {
	schema := schema.MustInfer(new(testObject))
	if dynamic {
		schema.UniqueKeys = true
	}
	_, err := client.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Recursive: true,
		Attributes: map[string]interface{}{
			"schema": schema,
		},
	})
	if err != nil {
		return xerrors.Errorf("unable to create node: %w", err)
	}

	writer, err := client.WriteTable(ctx, path, nil)
	if err != nil {
		return xerrors.Errorf("unable to create table writer: %w", err)
	}
	for _, x := range data {
		if err := writer.Write(x); err != nil {
			return xerrors.Errorf("unable to write: %w", err)
		}
	}
	if err := writer.Commit(); err != nil {
		return xerrors.Errorf("unable to commit writer: %w", err)
	}
	logger.Info("table created", log.Any("path", path))

	mrClient := mapreduce.New(client)

	sortSpec := spec.Sort()
	sortSpec.SortBy = []string{"key"}
	sortSpec.AddInput(path)
	sortSpec.SetOutput(path)
	logger.Info("sorting table", log.Any("path", path))
	operation, err := mrClient.Sort(sortSpec)
	if err != nil {
		return xerrors.Errorf("unable to start sort operation: %w", err)
	}
	if err := operation.Wait(); err != nil {
		return xerrors.Errorf("unable to sort table: %w", err)
	}
	logger.Info("table successfully sorted", log.Any("path", path))

	if dynamic {
		if err := client.AlterTable(ctx, path, &yt.AlterTableOptions{Dynamic: util.TruePtr()}); err != nil {
			return xerrors.Errorf("unable to alter table: %w", err)
		}
		if err := migrate.MountAndWait(ctx, client, path); err != nil {
			return xerrors.Errorf("unable to mount table: %w", err)
		}
		logger.Info("table successfully converted to dynamic", log.Any("path", path))
	}

	return nil
}

func createTables(ctx context.Context, client yt.Client, logger log.Logger, dynamic bool, tables map[ypath.Path][]testObject) error {
	errors := make(chan error)
	for path, data := range tables {
		go func(path ypath.Path, data []testObject) {
			errors <- createTable(ctx, client, logger, path, dynamic, data)
		}(path, data)
	}
	for range tables {
		err := <-errors
		if err != nil {
			return xerrors.Errorf("unable to create some table: %w", err)
		}
	}
	return nil
}

func mergeAndCheck(t *testing.T, ctx context.Context, client yt.Client, logger log.Logger, path ypath.Path, cleanupPath ypath.Path, prefix string, infix string, expected map[string][]testObject, tableRotationEnabled bool) {
	tableWriterSpec := map[string]interface{}{"max_row_weight": 128 * 1024 * 1024}
	err := ytsink.Merge(
		ctx, client, logger, path, cleanupPath, prefix, infix, "_tmp", yt2.ExePath, tableWriterSpec,
		func(schema schema.Schema) map[string]interface{} {
			return map[string]interface{}{
				"enable_dynamic_store_read": true,
			}
		}, tableRotationEnabled)
	require.NoError(t, err)
	attrs := testAttrs{EnableDynamicStoreRead: true}
	for name, data := range expected {
		check(t, ctx, client, logger, yt2.SafeChild(path, name), data, attrs)
	}
}

func check(
	t *testing.T,
	ctx context.Context,
	client yt.Client,
	logger log.Logger,
	path ypath.Path,
	expectedData []testObject,
	expectedAttrs testAttrs,
) {
	exists, err := client.NodeExists(ctx, path, nil)
	require.NoError(t, err)
	require.True(t, exists)

	table, err := client.ReadTable(ctx, path, nil)
	require.NoError(t, err)
	defer cleanup.Close(table, logger)

	var actualData []testObject
	for table.Next() {
		var x testObject
		require.NoError(t, table.Scan(&x))
		actualData = append(actualData, x)
	}
	require.NoError(t, table.Err())
	require.Equal(t, expectedData, actualData)

	var actualAttrs testAttrs
	require.NoError(t, client.GetNode(ctx, path.Attrs(), &actualAttrs, nil))
	require.Equal(t, expectedAttrs, actualAttrs)
}
