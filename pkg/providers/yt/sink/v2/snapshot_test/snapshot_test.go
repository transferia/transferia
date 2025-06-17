//go:build !disable_yt_provider

package snapshot_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	staticsink "github.com/transferia/transferia/pkg/providers/yt/sink/v2"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
)

var (
	testDstSchema = abstract.NewTableSchema(abstract.TableColumns{
		abstract.ColSchema{ColumnName: "author_id", DataType: schema.TypeString.String()},
		abstract.ColSchema{ColumnName: "id", DataType: schema.TypeInt32.String(), PrimaryKey: true},
		abstract.ColSchema{ColumnName: "is_deleted", DataType: schema.TypeBoolean.String()},
	})

	reducedDstSchema = abstract.NewTableSchema(abstract.TableColumns{
		abstract.ColSchema{ColumnName: "author_id", DataType: schema.TypeString.String()},
		abstract.ColSchema{ColumnName: "id", DataType: schema.TypeInt32.String(), PrimaryKey: true},
	})

	dstSample = yt.YtDestination{
		Path:          "//home/cdc/test/mock2yt",
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
		Cleanup:       model.DisabledCleanup,
		Static:        true,
	}

	trueConst  = true
	falseConst = false
)

type row struct {
	AuthorID  string `yson:"author_id"`
	ID        int32  `yson:"id"`
	IsDeleted *bool  `yson:"is_deleted"`
}

func TestYTStaticTableSink(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(dstSample.Cluster)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "YT DST", Port: targetPort}))
	}()

	t.Run("SingleSnapshotOneTable", singleSnapshotOneTable)
	t.Run("ShardedSnapshotManyTables", shardedSnapshotManyTables)
	t.Run("RetryingPartsOnError", retryingParts)
	t.Run("PushTwoTablesInOne", twoTablesInOne)
	t.Run("WithShuffledColumns", withShuffledColumns)
}

func singleSnapshotOneTable(t *testing.T) {
	ytEnv, cancel := recipe.NewEnv(t)
	defer cancel()

	cp := coordinator.NewStatefulFakeClient()

	dst := newYTDstModel(dstSample, false)
	tableName := "test_table_single_table"

	ok, err := ytEnv.YT.NodeExists(context.Background(), ypath.Path(fmt.Sprintf("%s/%s", dst.Path(), tableName)), nil)
	require.NoError(t, err)
	require.False(t, ok)

	itemsBuilder := helpers.NewChangeItemsBuilder("public", tableName, testDstSchema)

	// Without sorting
	// push items to non-existent table
	pushItems(t, cp, dst, [][]abstract.ChangeItem{
		itemsBuilder.InitShardedTableLoad(),
		itemsBuilder.InitTableLoad(),
		itemsBuilder.Inserts(t, []map[string]interface{}{{"id": 1, "author_id": "b", "is_deleted": false}}),
		itemsBuilder.Inserts(t, []map[string]interface{}{{"id": 0, "author_id": "a", "is_deleted": true}}),
		itemsBuilder.Inserts(t, []map[string]interface{}{{"id": 3, "author_id": "", "is_deleted": nil}}),
		itemsBuilder.DoneTableLoad(),
		itemsBuilder.DoneShardedTableLoad(),
	})

	checkData(t, dst, tableName, []row{
		{AuthorID: "a", ID: 0, IsDeleted: &trueConst},
		{AuthorID: "b", ID: 1, IsDeleted: &falseConst},
		{AuthorID: "", ID: 3, IsDeleted: nil},
	}, true)

	// push items to existent table
	pushItems(t, cp, dst, [][]abstract.ChangeItem{
		itemsBuilder.InitShardedTableLoad(),
		itemsBuilder.InitTableLoad(),
		itemsBuilder.Inserts(t, []map[string]interface{}{{"id": 5, "author_id": "x", "is_deleted": false}}),
		itemsBuilder.Inserts(t, []map[string]interface{}{{"id": 4, "author_id": "f", "is_deleted": true}}),
		itemsBuilder.DoneTableLoad(),
		itemsBuilder.DoneShardedTableLoad(),
	})

	checkData(t, dst, tableName, []row{
		{AuthorID: "a", ID: 0, IsDeleted: &trueConst},
		{AuthorID: "b", ID: 1, IsDeleted: &falseConst},
		{AuthorID: "", ID: 3, IsDeleted: nil},
		{AuthorID: "f", ID: 4, IsDeleted: &trueConst},
		{AuthorID: "x", ID: 5, IsDeleted: &falseConst},
	}, true)

	// With sorting
	// push items to existent table with Drop
	sample := dstSample
	sample.Cleanup = model.Drop
	dst = newYTDstModel(sample, true)
	pushItems(t, cp, dst, [][]abstract.ChangeItem{
		itemsBuilder.InitShardedTableLoad(),
		itemsBuilder.InitTableLoad(),
		itemsBuilder.Inserts(t, []map[string]interface{}{{"id": 5, "author_id": "x", "is_deleted": false}}),
		itemsBuilder.Inserts(t, []map[string]interface{}{{"id": 4, "author_id": "f", "is_deleted": true}}),
		itemsBuilder.DoneTableLoad(),
		itemsBuilder.DoneShardedTableLoad(),
	})

	checkData(t, dst, tableName, []row{
		{AuthorID: "f", ID: 4, IsDeleted: &trueConst},
		{AuthorID: "x", ID: 5, IsDeleted: &falseConst},
	}, false)

	// push unsorted table to existent sorted
	dst = newYTDstModel(dstSample, false)
	sink, err := staticsink.NewStaticSink(dst, cp, helpers.TransferID, helpers.EmptyRegistry(), logger.Log)
	require.NoError(t, err)

	require.NoError(t, sink.Push(itemsBuilder.InitShardedTableLoad()))
	require.NoError(t, sink.Push(itemsBuilder.InitTableLoad()))
	require.NoError(t, sink.Push(itemsBuilder.Inserts(t, []map[string]interface{}{{"id": 7, "author_id": "xxx", "is_deleted": false}})))
	require.NoError(t, sink.Push(itemsBuilder.Inserts(t, []map[string]interface{}{{"id": 6, "author_id": "xx", "is_deleted": true}})))
	require.NoError(t, sink.Push(itemsBuilder.DoneTableLoad()))

	require.Error(t, sink.Push(itemsBuilder.DoneShardedTableLoad()))
}

func shardedSnapshotManyTables(t *testing.T) {
	cp := coordinator.NewStatefulFakeClient()

	sample := dstSample
	sample.Cleanup = model.Drop
	dst := newYTDstModel(sample, true)

	firstTableName := "test_sharded_table_1"
	secondTableName := "test_sharded_table_2"

	firstItemsBuilder := helpers.NewChangeItemsBuilder("public", firstTableName, testDstSchema)
	secondItemsBuilder := helpers.NewChangeItemsBuilder("public", secondTableName, reducedDstSchema)

	// push InitShTableLoad items
	primarySink, err := staticsink.NewStaticSink(dst, cp, helpers.TransferID, helpers.EmptyRegistry(), logger.Log)
	require.NoError(t, err)

	require.NoError(t, primarySink.Push(firstItemsBuilder.InitShardedTableLoad()))
	require.NoError(t, primarySink.Push(secondItemsBuilder.InitShardedTableLoad()))

	// push Inserts to sinks on secondary workers
	secondarySink, err := staticsink.NewStaticSink(dst, cp, helpers.TransferID, helpers.EmptyRegistry(), logger.Log)
	require.NoError(t, err)

	require.NoError(t, secondarySink.Push(firstItemsBuilder.InitTableLoad()))
	require.NoError(t, secondarySink.Push(firstItemsBuilder.Inserts(t, []map[string]interface{}{{"id": 2, "author_id": "222", "is_deleted": true}})))
	require.NoError(t, secondarySink.Push(firstItemsBuilder.Inserts(t, []map[string]interface{}{{"id": 1, "author_id": "111", "is_deleted": false}})))
	require.NoError(t, secondarySink.Push(firstItemsBuilder.DoneTableLoad()))

	secondarySink, err = staticsink.NewStaticSink(dst, cp, helpers.TransferID, helpers.EmptyRegistry(), logger.Log)
	require.NoError(t, err)

	require.NoError(t, secondarySink.Push(secondItemsBuilder.InitTableLoad()))
	require.NoError(t, secondarySink.Push(secondItemsBuilder.Inserts(t, []map[string]interface{}{{"id": 0, "author_id": "000"}, {"id": 1, "author_id": "111"}})))
	require.NoError(t, secondarySink.Push(secondItemsBuilder.Inserts(t, []map[string]interface{}{{"id": 3, "author_id": "333"}})))
	require.NoError(t, secondarySink.Push(secondItemsBuilder.DoneTableLoad()))

	// push DoneShTableLoad items and complete snapshot
	primarySink, err = staticsink.NewStaticSink(dst, cp, helpers.TransferID, helpers.EmptyRegistry(), logger.Log)
	require.NoError(t, err)

	require.NoError(t, primarySink.Push(firstItemsBuilder.DoneShardedTableLoad()))
	require.NoError(t, primarySink.Push(secondItemsBuilder.DoneShardedTableLoad()))

	completable, ok := primarySink.(abstract.Committable)
	require.True(t, ok)
	require.NoError(t, completable.Commit())

	// check result
	checkData(t, dst, firstTableName, []row{
		{AuthorID: "111", ID: 1, IsDeleted: &falseConst},
		{AuthorID: "222", ID: 2, IsDeleted: &trueConst},
	}, false)
	checkData(t, dst, secondTableName, []row{
		{AuthorID: "000", ID: 0},
		{AuthorID: "111", ID: 1},
		{AuthorID: "333", ID: 3},
	}, false)
}

func retryingParts(t *testing.T) {
	ytEnv, cancel := recipe.NewEnv(t)
	defer cancel()

	cp := coordinator.NewStatefulFakeClient()

	dst := newYTDstModel(dstSample, false)
	tableName := "test_table_retry"

	ok, err := ytEnv.YT.NodeExists(context.Background(), ypath.Path(fmt.Sprintf("%s/%s", dst.Path(), tableName)), nil)
	require.NoError(t, err)
	require.False(t, ok)

	itemsBuilder := helpers.NewChangeItemsBuilder("public", tableName, testDstSchema)

	currentSink, err := staticsink.NewStaticSink(dst, cp, helpers.TransferID, helpers.EmptyRegistry(), logger.Log)
	require.NoError(t, err)
	require.NoError(t, currentSink.Push(itemsBuilder.InitShardedTableLoad()))

	currentSink, err = staticsink.NewStaticSink(dst, cp, helpers.TransferID, helpers.EmptyRegistry(), logger.Log)
	require.NoError(t, err)
	require.NoError(t, currentSink.Push(itemsBuilder.InitTableLoad()))
	require.Error(t, currentSink.Push(itemsBuilder.Inserts(t, []map[string]interface{}{{"author_id": 123, "is_deleted": 15}})))
	require.NoError(t, currentSink.Push(itemsBuilder.DoneTableLoad()))

	currentSink, err = staticsink.NewStaticSink(dst, cp, helpers.TransferID, helpers.EmptyRegistry(), logger.Log)
	require.NoError(t, err)
	require.NoError(t, currentSink.Push(itemsBuilder.InitTableLoad()))
	require.NoError(t, currentSink.Push(itemsBuilder.Inserts(t, []map[string]interface{}{{"id": 0, "author_id": "a", "is_deleted": true}})))
	require.NoError(t, currentSink.Push(itemsBuilder.DoneTableLoad()))

	currentSink, err = staticsink.NewStaticSink(dst, cp, helpers.TransferID, helpers.EmptyRegistry(), logger.Log)
	require.NoError(t, err)
	require.NoError(t, currentSink.Push(itemsBuilder.DoneShardedTableLoad()))

	completable, ok := currentSink.(abstract.Committable)
	require.True(t, ok)
	require.NoError(t, completable.Commit())

	checkData(t, dst, tableName, []row{
		{AuthorID: "a", ID: 0, IsDeleted: &trueConst},
	}, true)
}

func twoTablesInOne(t *testing.T) {
	ytEnv, cancel := recipe.NewEnv(t)
	defer cancel()

	cp := coordinator.NewStatefulFakeClient()

	transferID := "test_two_in_one"
	firstTableName := "first_table"
	secondTableName := "second_table"

	dstCfg := dstSample
	dstCfg.AltNames = map[string]string{
		secondTableName: firstTableName,
	}
	dst := newYTDstModel(dstCfg, false)

	ok, err := ytEnv.YT.NodeExists(context.Background(), ypath.Path(fmt.Sprintf("%s/%s", dst.Path(), firstTableName)), nil)
	require.NoError(t, err)
	require.False(t, ok)

	itemsBuilderFirstTable := helpers.NewChangeItemsBuilder("public", firstTableName, testDstSchema)
	itemsBuilderSecondTable := helpers.NewChangeItemsBuilder("public", secondTableName, testDstSchema)

	pushItemsWithoutCommit(t, cp, transferID, dst, [][]abstract.ChangeItem{
		itemsBuilderFirstTable.InitShardedTableLoad(),
		itemsBuilderSecondTable.InitShardedTableLoad(),
	})

	pushItemsWithoutCommit(t, cp, transferID, dst, [][]abstract.ChangeItem{
		itemsBuilderFirstTable.InitTableLoad(),
		itemsBuilderFirstTable.Inserts(t, []map[string]interface{}{{"id": 0, "author_id": "a", "is_deleted": true}}),
		itemsBuilderFirstTable.Inserts(t, []map[string]interface{}{{"id": 3, "author_id": "", "is_deleted": nil}}),
		itemsBuilderFirstTable.DoneTableLoad(),
	})

	pushItemsWithoutCommit(t, cp, transferID, dst, [][]abstract.ChangeItem{
		itemsBuilderFirstTable.InitTableLoad(),
		itemsBuilderFirstTable.Inserts(t, []map[string]interface{}{{"id": 1, "author_id": "b", "is_deleted": false}}),
		itemsBuilderFirstTable.DoneTableLoad(),
	})

	pushItemsWithoutCommit(t, cp, transferID, dst, [][]abstract.ChangeItem{
		itemsBuilderFirstTable.DoneShardedTableLoad(),
		itemsBuilderSecondTable.DoneShardedTableLoad(),
	})

	commit(t, cp, transferID, dst)

	checkData(t, dst, firstTableName, []row{
		{AuthorID: "a", ID: 0, IsDeleted: &trueConst},
		{AuthorID: "b", ID: 1, IsDeleted: &falseConst},
		{AuthorID: "", ID: 3, IsDeleted: nil},
	}, true)
}

func withShuffledColumns(t *testing.T) {
	ytEnv, cancel := recipe.NewEnv(t)
	defer cancel()

	cp := coordinator.NewStatefulFakeClient()

	dst := newYTDstModel(dstSample, true)
	tableName := "test_table_shuffled"

	ok, err := ytEnv.YT.NodeExists(context.Background(), ypath.Path(fmt.Sprintf("%s/%s", dst.Path(), tableName)), nil)
	require.NoError(t, err)
	require.False(t, ok)

	itemsBuilder := helpers.NewChangeItemsBuilder("public", tableName, testDstSchema)

	pushItems(t, cp, dst, [][]abstract.ChangeItem{
		itemsBuilder.InitShardedTableLoad(),
		itemsBuilder.InitTableLoad(),
		itemsBuilder.Inserts(t, []map[string]interface{}{{"id": 0, "author_id": "000", "is_deleted": true}}),
		itemsBuilder.DoneTableLoad(),
		itemsBuilder.DoneShardedTableLoad(),
	})
	checkData(t, dst, tableName, []row{
		{AuthorID: "000", ID: 0, IsDeleted: &trueConst},
	}, false)

	pushItems(t, cp, dst, [][]abstract.ChangeItem{
		itemsBuilder.InitShardedTableLoad(),
		itemsBuilder.InitTableLoad(),
		itemsBuilder.Inserts(t, []map[string]interface{}{{"id": 2, "author_id": "002", "is_deleted": false}}),
		itemsBuilder.Inserts(t, []map[string]interface{}{{"author_id": "001", "id": 1}}),
		itemsBuilder.DoneTableLoad(),
		itemsBuilder.DoneShardedTableLoad(),
	})

	checkData(t, dst, tableName, []row{
		{AuthorID: "000", ID: 0, IsDeleted: &trueConst},
		{AuthorID: "001", ID: 1, IsDeleted: nil},
		{AuthorID: "002", ID: 2, IsDeleted: &falseConst},
	}, false)
}

func pushItemsWithoutCommit(t *testing.T, cp coordinator.Coordinator, transferID string, dst yt.YtDestinationModel, input [][]abstract.ChangeItem) {
	currentSink, err := staticsink.NewStaticSink(dst, cp, transferID, helpers.EmptyRegistry(), logger.Log)
	require.NoError(t, err)

	for _, items := range input {
		require.NoError(t, currentSink.Push(items))
	}
}

func commit(t *testing.T, cp coordinator.Coordinator, transferID string, dst yt.YtDestinationModel) {
	currentSink, err := staticsink.NewStaticSink(dst, cp, transferID, helpers.EmptyRegistry(), logger.Log)
	require.NoError(t, err)

	completable, ok := currentSink.(abstract.Committable)
	require.True(t, ok)
	require.NoError(t, completable.Commit())
}

func pushItems(t *testing.T, cp coordinator.Coordinator, dst yt.YtDestinationModel, input [][]abstract.ChangeItem) {
	currentSink, err := staticsink.NewStaticSink(dst, cp, helpers.TransferID, helpers.EmptyRegistry(), logger.Log)
	require.NoError(t, err)

	for _, items := range input {
		require.NoError(t, currentSink.Push(items))
	}

	completable, ok := currentSink.(abstract.Committable)
	require.True(t, ok)
	require.NoError(t, completable.Commit())
}

func checkData(t *testing.T, dst yt.YtDestinationModel, tableName string, expected []row, needSortRes bool) {
	ytClient, err := ytclient.FromConnParams(dst, logger.Log)
	require.NoError(t, err)
	rows, err := ytClient.ReadTable(context.Background(), ypath.Path(dst.Path()+"/"+tableName), nil)
	require.NoError(t, err)

	var res []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r))
		res = append(res, r)
	}

	if needSortRes {
		sort.Slice(res, func(i int, j int) bool {
			return res[i].ID < res[j].ID
		})
	}

	require.Equal(t, res, expected)
}

func newYTDstModel(cfg yt.YtDestination, allowSorting bool) yt.YtDestinationModel {
	cfg.SortedStatic = allowSorting
	dst := yt.NewYtDestinationV1(cfg)
	dst.WithDefaults()

	return dst
}
