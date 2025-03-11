package sink

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/internal/metrics"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/cleanup"
	yt2 "github.com/transferria/transferria/pkg/providers/yt"
	ytclient "github.com/transferria/transferria/pkg/providers/yt/client"
	"github.com/transferria/transferria/pkg/providers/yt/recipe"
	"github.com/transferria/transferria/pkg/stats"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

const (
	DefaultTimeout = 3 * time.Minute
)

// test runner
func TestSingleStaticTable(t *testing.T) {
	t.Run("Simple", simple)
	t.Run("NilConfigNotAllowed", nilConfigNotAllowed)
	t.Run("RepeatedValuesInKeyColumnOfSortedTableAreAllowed", repeatedValuesInKeyColumnOfSortedTableAreAllowed)
	t.Run("AbsentSchemaKeysInSortedTableIsNotAllowed", absentSchemaKeysInSortedTableIsNotAllowed)
	t.Run("RepeatedKeysInOrderedTableIsAllowed", repeatedKeysInOrderedTableIsAllowed)
	t.Run("AbsentSchemaKeysInOrderedTableIsAllowed", absentSchemaKeysInOrderedTableIsAllowed)
	t.Run("SchemaExtensionIsAllowed", schemaExtensionIsAllowed)
	t.Run("WrongChangeItemsAreIgnored", wrongChangeItemsAreIgnored)
	t.Run("TableAppearsAtomicallyAfterCommit", tableAppearsAtomicallyAfterCommit)
	t.Run("TableWithOnlyPrimaryKeys", tableWithOnlyPrimaryKeys)
}

// Utilities

// Haskell-like map :: (a -> b) -> [a] -> [b]
func mapValuesToChangeItems(f func([]interface{}) abstract.ChangeItem) func([][]interface{}) []abstract.ChangeItem {
	return func(ls [][]interface{}) []abstract.ChangeItem {
		var result []abstract.ChangeItem
		for _, l := range ls {
			result = append(result, f(l))
		}
		return result
	}
}

// initializes YT client and sinker config
func initYt(t *testing.T, path string) (testCfg yt2.YtDestinationModel, client yt.Client) {
	cfg := yt2.NewYtDestinationV1(yt2.YtDestination{
		Path:          path,
		Cluster:       os.Getenv("YT_PROXY"),
		PrimaryMedium: "default",
		CellBundle:    "default",
		Spec:          *yt2.NewYTSpec(map[string]interface{}{"max_row_weight": 128 * 1024 * 1024}),
		CustomAttributes: map[string]string{
			"test":               "%true",
			"expiration_timeout": "604800000",
			"expiration_time":    "\"2200-01-12T03:32:51.298047Z\"",
		},
	})
	cfg.WithDefaults()

	cl, err := ytclient.FromConnParams(cfg, logger.Log)
	require.NoError(t, err)
	return cfg, cl
}

// create schema config and change items generator common for tests
// Schema 1: key-value schema where key is designated as primary key
type arcWarden struct {
	Key int32  `yson:"key"`
	Val string `yson:"val"`
}

var arcWardenSchema = abstract.NewTableSchema([]abstract.ColSchema{
	{DataType: "int32", ColumnName: "key", PrimaryKey: true},
	{DataType: "any", ColumnName: "val"},
})
var toArcWardenItems = mapValuesToChangeItems(func(values []interface{}) abstract.ChangeItem {
	return abstract.ChangeItem{
		TableSchema:  arcWardenSchema,
		Kind:         abstract.InsertKind,
		Schema:       "arc",
		Table:        "warden",
		ColumnNames:  []string{"key", "val"},
		ColumnValues: values,
	}
})

// Schema 2: the same scheme, but 'key' is not designated as primary
type withoutKeys struct {
	Key int32  `yson:"key"`
	Val string `yson:"val"`
}

var withoutKeysSchema = abstract.NewTableSchema([]abstract.ColSchema{
	{DataType: "int32", ColumnName: "key"},
	{DataType: "any", ColumnName: "val"},
})
var toWithoutKeysItems = mapValuesToChangeItems(func(values []interface{}) abstract.ChangeItem {
	return abstract.ChangeItem{
		TableSchema:  withoutKeysSchema,
		Kind:         abstract.InsertKind,
		Schema:       "without",
		Table:        "keys",
		ColumnNames:  []string{"key", "val"},
		ColumnValues: values,
	}
})

// Schema 3: all keys schema
type allKeys struct {
	Key int32 `yson:"K"`
	Val int32 `yson:"V"`
}

var allKeysSchema = abstract.NewTableSchema([]abstract.ColSchema{
	{DataType: "int32", ColumnName: "K", PrimaryKey: true},
	{DataType: "int32", ColumnName: "V", PrimaryKey: true},
})
var toAllKeysSchema = mapValuesToChangeItems(func(values []interface{}) abstract.ChangeItem {
	return abstract.ChangeItem{
		TableSchema:  allKeysSchema,
		Kind:         abstract.InsertKind,
		Schema:       "all",
		Table:        "keys",
		ColumnNames:  []string{"K", "V"},
		ColumnValues: values,
	}
})

// tests
func simple(t *testing.T) {
	// create single static table for change item consumption
	path := ypath.Path("//home/cdc/test/TM-1572")
	cfg, ytClient := initYt(t, path.String())
	defer ytClient.Stop()
	defer teardown(ytClient, path)
	// schema might be unknown during initialization
	sinkStats := stats.NewSinkerStats(metrics.NewRegistry())
	tableName := "arc_warden_simple_test"
	sst, err := NewSingleStaticTable(ytClient, ypath.Path(cfg.Path()), tableName, arcWardenSchema.Columns(), cfg, 0, "", model.DisabledCleanup, sinkStats, logger.Log, yt2.ExePath)
	require.NoError(t, err)

	// write some change items
	err = sst.Write(
		toArcWardenItems([][]interface{}{
			{int32(1), "some"},
			{int32(2), "body"},
			{int32(3), "once"},
			{int32(4), "told"},
		}))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	// commit changes
	err = sst.Commit(ctx)
	require.NoError(t, err)
	// convert table to dynamic
	err = sst.finishLoading()
	require.NoError(t, err)

	// load result from YT
	rows, err := ytClient.SelectRows(ctx, fmt.Sprintf("* from [%v/%v]", cfg.Path(), tableName), nil)
	require.NoError(t, err)
	var res []arcWarden
	for rows.Next() {
		var row arcWarden
		require.NoError(t, rows.Scan(&row))
		res = append(res, row)
	}
	// sort answer to preserve order
	sort.Slice(res, func(i, j int) bool {
		return res[i].Key < res[j].Key
	})
	require.Equal(t, []arcWarden{
		{int32(1), "some"},
		{int32(2), "body"},
		{int32(3), "once"},
		{int32(4), "told"},
	}, res)
}

func nilConfigNotAllowed(t *testing.T) {
	cfg, ytClient := initYt(t, "//home/cdc/test/TM-1572")
	defer ytClient.Stop()
	defer teardown(ytClient, "//home/cdc/test/TM-1572")
	// schema might be unknown during initialization
	sinkStats := stats.NewSinkerStats(metrics.NewRegistry())
	_, err := NewSingleStaticTable(ytClient, ypath.Path(cfg.Path()), "nil_not_allowed", []abstract.ColSchema{}, nil, 0, "", model.DisabledCleanup, sinkStats, logger.Log, yt2.ExePath)
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "parameter 'cfg *server.ytdestination' should not be 'nil'")
}

func repeatedValuesInKeyColumnOfSortedTableAreAllowed(t *testing.T) {
	// this test is connected with ticket: https://st.yandex-team.ru/YT-15045
	// create single static table for change item consumption
	path := ypath.Path("//home/cdc/test/TM-1572")
	cfg, ytClient := initYt(t, path.String())
	defer ytClient.Stop()
	defer teardown(ytClient, path)
	// schema might be unknown during initialization
	sinkStats := stats.NewSinkerStats(metrics.NewRegistry())
	tableName := "arc_warden_repeated_values_in_key_column_of_sorted_table_not_allowed"
	sst, err := NewSingleStaticTable(ytClient, ypath.Path(cfg.Path()), tableName, arcWardenSchema.Columns(), cfg, 0, "", model.DisabledCleanup, sinkStats, logger.Log, yt2.ExePath)
	require.NoError(t, err)

	// write some change items
	err = sst.Write(
		toArcWardenItems([][]interface{}{
			{int32(1), "over"},
			{int32(127), "and over"},
			{int32(239), "and again"},
			{int32(127), "and over"},
			{int32(1), "over"},
			{int32(239), "and again"},
		}))
	require.NoError(t, err)

	// commit changes
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()
	err = sst.Commit(ctx)
	require.NoError(t, err)

	reader, err := ytClient.ReadTable(ctx, yt2.SafeChild(path, sst.mergedTableName), nil)
	require.NoError(t, err)
	defer cleanup.Close(reader, logger.Log)
	var items []*arcWarden
	for reader.Next() {
		item := new(arcWarden)
		require.NoError(t, reader.Scan(item))
		items = append(items, item)
	}
	require.Equal(t, []*arcWarden{
		{Key: 1, Val: "over"},
		{Key: 1, Val: "over"},
		{Key: 127, Val: "and over"},
		{Key: 127, Val: "and over"},
		{Key: 239, Val: "and again"},
		{Key: 239, Val: "and again"},
	}, items)

	require.NoError(t, sst.finishLoading())
	reader, err = ytClient.ReadTable(ctx, yt2.SafeChild(path, tableName), nil)
	require.NoError(t, err)
	defer cleanup.Close(reader, logger.Log)
	items = nil
	for reader.Next() {
		item := new(arcWarden)
		require.NoError(t, reader.Scan(item))
		items = append(items, item)
	}
	require.Equal(t, []*arcWarden{
		{Key: 1, Val: "over"},
		{Key: 127, Val: "and over"},
		{Key: 239, Val: "and again"},
	}, items)
}

func absentSchemaKeysInSortedTableIsNotAllowed(t *testing.T) {
	// create single static table for change item consumption
	path := ypath.Path("//home/cdc/test/TM-1572")
	cfg, ytClient := initYt(t, path.String())
	defer ytClient.Stop()
	defer teardown(ytClient, path)
	// schema might be unknown during initialization
	sinkStats := stats.NewSinkerStats(metrics.NewRegistry())
	tableName := "schema_without_keys_not_allowed_in_sorted_table"
	sst, err := NewSingleStaticTable(ytClient, ypath.Path(cfg.Path()), tableName, withoutKeysSchema.Columns(), cfg, 0, "", model.DisabledCleanup, sinkStats, logger.Log, yt2.ExePath)
	require.NoError(t, err)

	// even if there is no repetition in source data we should have been provided with certain primary key
	err = sst.Write(
		toWithoutKeysItems([][]interface{}{
			{int32(1007), "aw, sheet"},
			{int32(1008), "here we go again"},
			{int32(1009), "worst place in the world"},
		}))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()
	err = sst.Commit(ctx)
	require.Error(t, err)

	require.Contains(t, strings.ToLower(err.Error()), "no key columns found")
}

func repeatedKeysInOrderedTableIsAllowed(t *testing.T) {
	// create single static table for change item consumption
	path := ypath.Path("//home/cdc/test/TM-1572")
	cfg, ytClient := initYt(t, path.String())
	cfg.SetOrdered() // make ordered table
	defer ytClient.Stop()
	defer teardown(ytClient, path)
	// schema might be unknown during initialization
	sinkStats := stats.NewSinkerStats(metrics.NewRegistry())
	tableName := "repeated_keys_in_ordered_table_is_allowed"
	sst, err := NewSingleStaticTable(ytClient, ypath.Path(cfg.Path()), tableName, withoutKeysSchema.Columns(), cfg, 0, "", model.DisabledCleanup, sinkStats, logger.Log, yt2.ExePath)
	require.NoError(t, err)

	// write some change items
	err = sst.Write(
		toWithoutKeysItems([][]interface{}{
			{int32(3), ">>>"},
			{int32(2), "to"},
			{int32(2), "be"},
			{int32(9), "continued"},
			{int32(3), ">>>"},
		}))
	require.NoError(t, err)

	// commit changes
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()
	err = sst.Commit(ctx)
	require.NoError(t, err)

	// convert table to dynamic
	err = sst.finishLoading()
	require.NoError(t, err)

	// load result from YT
	rows, err := ytClient.SelectRows(ctx, fmt.Sprintf("* from [%v/%v]", cfg.Path(), tableName), nil)
	require.NoError(t, err)
	var res []withoutKeys
	for rows.Next() {
		var row withoutKeys
		require.NoError(t, rows.Scan(&row))
		res = append(res, row)
	}
	// TODO should we sort result here? Table is ordered
	require.Equal(t, []withoutKeys{
		{int32(3), ">>>"},
		{int32(2), "to"},
		{int32(2), "be"},
		{int32(9), "continued"},
		{int32(3), ">>>"},
	}, res)
}

func absentSchemaKeysInOrderedTableIsAllowed(t *testing.T) {
	// create single static table for change item consumption
	path := ypath.Path("//home/cdc/test/TM-1572")
	cfg, ytClient := initYt(t, path.String())
	defer ytClient.Stop()
	defer teardown(ytClient, path)
	// schema might be unknown during initialization
	sinkStats := stats.NewSinkerStats(metrics.NewRegistry())
	tableName := "schema_without_keys_allowed_in_ordered_table"
	sst, err := NewSingleStaticTable(ytClient, ypath.Path(cfg.Path()), tableName, withoutKeysSchema.Columns(), cfg, 0, "", model.DisabledCleanup, sinkStats, logger.Log, yt2.ExePath)
	require.NoError(t, err)

	// even if there is no repetition in source data we should have been provided with certain primary key
	err = sst.Write(
		toWithoutKeysItems([][]interface{}{
			{int32(3321), "FIRE"},
			{int32(6194), "IN A"},
			{int32(2579), "HOLE"},
		}))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()
	err = sst.Commit(ctx)
	require.Error(t, err)

	require.Contains(t, strings.ToLower(err.Error()), "no key columns found")
}

func schemaExtensionIsAllowed(t *testing.T) {
	// create single static table for change item consumption
	path := ypath.Path("//home/cdc/test/TM-1572")
	cfg, ytClient := initYt(t, path.String())
	defer ytClient.Stop()
	defer teardown(ytClient, path)
	// schema might be unknown during initialization
	sinkStats := stats.NewSinkerStats(metrics.NewRegistry())
	tableName := "arc_warden_ext"
	sst, err := NewSingleStaticTable(ytClient, ypath.Path(cfg.Path()), tableName, arcWardenSchema.Columns(), cfg, 0, "", model.DisabledCleanup, sinkStats, logger.Log, yt2.ExePath)
	require.NoError(t, err)

	arcWardenSchemaExtended := []abstract.ColSchema{
		{DataType: "int32", ColumnName: "key", PrimaryKey: true},
		{DataType: "any", ColumnName: "val"},
		{DataType: "string", ColumnName: "buzz"},
	}

	// write some change items
	err = sst.Write(
		toArcWardenItems([][]interface{}{
			{int32(1), "they"},
			{int32(2), "hate"},
			{int32(3), "fake"},
			{int32(4), "news"},
		}))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	// extend schema
	sst.UpdateSchema(arcWardenSchemaExtended)
	// commit changes
	err = sst.Commit(ctx)
	require.NoError(t, err)
	// convert table to dynamic
	err = sst.finishLoading()
	require.NoError(t, err)

	// load result from YT
	type arcWardenExt struct {
		Key  int32  `yson:"key"`
		Val  string `yson:"val"`
		Buzz string `yson:"buzz"`
	}
	rows, err := ytClient.SelectRows(ctx, fmt.Sprintf("* from [%v/%v]", cfg.Path(), tableName), nil)
	require.NoError(t, err)
	var res []arcWardenExt
	for rows.Next() {
		var row arcWardenExt
		require.NoError(t, rows.Scan(&row))
		res = append(res, row)
	}
	// sort result by the key for consistency
	sort.Slice(res, func(i, j int) bool {
		return res[i].Key < res[j].Key
	})
	require.Equal(t, []arcWardenExt{
		{int32(1), "they", ""},
		{int32(2), "hate", ""},
		{int32(3), "fake", ""},
		{int32(4), "news", ""},
	}, res)
}

func wrongChangeItemsAreIgnored(t *testing.T) {
	// create single static table for change item consumption
	path := ypath.Path("//home/cdc/test/TM-1572")
	cfg, ytClient := initYt(t, path.String())
	defer ytClient.Stop()
	defer teardown(ytClient, path)
	// schema might be unknown during initialization
	sinkStats := stats.NewSinkerStats(metrics.NewRegistry())
	tableName := "arc_warden_ignore_change_items"
	sst, err := NewSingleStaticTable(ytClient, ypath.Path(cfg.Path()), tableName, arcWardenSchema.Columns(), cfg, 0, "", model.DisabledCleanup, sinkStats, logger.Log, yt2.ExePath)
	require.NoError(t, err)

	// write some change items
	changeItems := toArcWardenItems(
		[][]interface{}{
			{int32(0), "C"},
			{int32(1), "O"},
			{int32(2), "M"},
			{int32(3), "E"},
			{int32(4), " "},
			{int32(5), "T"},
			{int32(6), "O"},
			{int32(7), " "},
			{int32(8), "Y"},
			{int32(9), "A"},
			{int32(10), "N"},
			{int32(11), "D"},
			{int32(12), "E"},
			{int32(13), "X"},
		})
	changeItems[2].Kind = abstract.UpdateKind
	changeItems[3].Kind = abstract.DeleteKind
	changeItems[4].Kind = abstract.DDLKind
	changeItems[5].Kind = abstract.DoneTableLoad
	changeItems[6].Kind = abstract.MongoDropKind
	changeItems[7].Kind = abstract.InitTableLoad
	changeItems[8].Kind = abstract.MongoRenameKind
	changeItems[9].Kind = abstract.PgDDLKind
	changeItems[10].Kind = abstract.TruncateTableKind
	changeItems[13].Kind = abstract.MongoDropDatabaseKind

	err = sst.Write(changeItems)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	// commit changes
	err = sst.Commit(ctx)
	require.NoError(t, err)
	// convert table to dynamic
	err = sst.finishLoading()
	require.NoError(t, err)

	// load result from YT
	rows, err := ytClient.SelectRows(ctx, fmt.Sprintf("* from [%v/%v]", cfg.Path(), tableName), nil)
	require.NoError(t, err)
	var res []arcWarden
	for rows.Next() {
		var row arcWarden
		require.NoError(t, rows.Scan(&row))
		res = append(res, row)
	}
	// sort answer for consistency
	sort.Slice(res, func(i, j int) bool {
		return res[i].Key < res[j].Key
	})
	require.Equal(t, []arcWarden{
		{int32(00), "C"},
		{int32(01), "O"},
		{int32(11), "D"},
		{int32(12), "E"},
	}, res)
}

func tableAppearsAtomicallyAfterCommit(t *testing.T) {
	// create single static table for change item consumption
	path := ypath.Path("//home/cdc/test/TM-1572")
	cfg, ytClient := initYt(t, path.String())
	defer ytClient.Stop()
	defer teardown(ytClient, path)
	// schema might be unknown during initialization
	sinkStats := stats.NewSinkerStats(metrics.NewRegistry())
	tableName := "arc_warden_appears_atomically"
	sst, err := NewSingleStaticTable(ytClient, ypath.Path(cfg.Path()), tableName, arcWardenSchema.Columns(), cfg, 0, "", model.DisabledCleanup, sinkStats, logger.Log, yt2.ExePath)
	require.NoError(t, err)

	// write some change items
	err = sst.Write(
		toArcWardenItems([][]interface{}{
			{int32(-1), "hello, darkness, my old friend"},
			{int32(-2), "i've come to talk to you again"},
		}))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	// check that table is not yet exists
	exists, err := ytClient.NodeExists(ctx, yt2.SafeChild(ypath.Path(cfg.Path()), tableName), nil)
	require.NoError(t, err)
	require.False(t, exists, "table should not yet exist before commit")
	// commit changes
	err = sst.Commit(ctx)
	require.NoError(t, err)
	// check that table appeared
	exists, err = ytClient.NodeExists(ctx, yt2.SafeChild(ypath.Path(cfg.Path()), sst.mergedTableName), nil)
	require.NoError(t, err)
	require.True(t, exists, "table should exist after commit")
}

func tableWithOnlyPrimaryKeys(t *testing.T) {
	path := ypath.Path("//home/cdc/test/TM-1572")
	cfg, ytClient := initYt(t, path.String())
	defer ytClient.Stop()
	defer teardown(ytClient, path)

	sinkStats := stats.NewSinkerStats(metrics.NewRegistry())
	tableName := "all_keys_test"
	sst, err := NewSingleStaticTable(ytClient, ypath.Path(cfg.Path()), tableName, allKeysSchema.Columns(), cfg, 0, "", model.DisabledCleanup, sinkStats, logger.Log, yt2.ExePath)
	require.NoError(t, err)

	err = sst.Write(
		toAllKeysSchema([][]interface{}{
			{int32(1), int32(17)},
			{int32(2), int32(18)},
			{int32(3), int32(19)},
			{int32(4), int32(20)},
		}))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	err = sst.Commit(ctx)
	require.NoError(t, err)
	// before conversion we should check that schema has at least one non-key column
	err = sst.finishLoading()
	require.NoError(t, err)

	// load result from YT
	rows, err := ytClient.SelectRows(ctx, fmt.Sprintf("* from [%v/%v]", cfg.Path(), tableName), nil)
	require.NoError(t, err)
	var res []allKeys
	for rows.Next() {
		var row allKeys
		require.NoError(t, rows.Scan(&row))
		res = append(res, row)
	}
	// apply sorting for consistency
	sort.Slice(res, func(i, j int) bool {
		return res[i].Key < res[j].Key
	})
	require.Equal(t, []allKeys{
		{int32(1), int32(17)},
		{int32(2), int32(18)},
		{int32(3), int32(19)},
		{int32(4), int32(20)},
	}, res)
}

func TestCustomAttributesSingleStaticTable(t *testing.T) {
	if recipe.TestContainerEnabled() {
		t.Skip()
	}
	schema_ := abstract.NewTableSchema([]abstract.ColSchema{
		{
			DataType:   "double",
			ColumnName: "test",
			PrimaryKey: true,
		},
	})
	cfg := yt2.NewYtDestinationV1(yt2.YtDestination{
		Atomicity:                yt.AtomicityFull,
		CellBundle:               "default",
		PrimaryMedium:            "default",
		CustomAttributes:         map[string]string{"test": "%true"},
		Path:                     "//home/cdc/test/generic/temp",
		UseStaticTableOnSnapshot: true,
		Cluster:                  os.Getenv("YT_PROXY")},
	)
	cfg.WithDefaults()
	table, err := newSinker(cfg, "some_uniq_transfer_id", 0, logger.Log, metrics.NewRegistry(), coordinator.NewFakeClient())
	require.NoError(t, err)
	require.NoError(t, table.Push([]abstract.ChangeItem{{
		TableSchema: schema_,
		Kind:        abstract.InitShardedTableLoad,
		Table:       "test_table",
	}}))
	require.NoError(t, table.Push([]abstract.ChangeItem{{
		TableSchema: schema_,
		Kind:        abstract.InitTableLoad,
		Table:       "test_table",
	}}))
	require.NoError(t, table.Push([]abstract.ChangeItem{
		{
			TableSchema:  schema_,
			Kind:         abstract.InsertKind,
			ColumnNames:  []string{"test"},
			ColumnValues: []interface{}{3.99},
			Table:        "test_table",
		},
	}))
	require.NoError(t, table.Push([]abstract.ChangeItem{{
		TableSchema: schema_,
		Kind:        abstract.DoneTableLoad,
		Table:       "test_table",
	}}))
	require.NoError(t, table.Push([]abstract.ChangeItem{{
		TableSchema: schema_,
		Kind:        abstract.DoneShardedTableLoad,
		Table:       "test_table",
	}}))

	ytClient, err := ytclient.FromConnParams(cfg, logger.Log)
	require.NoError(t, err)
	var data bool
	require.NoError(t, ytClient.GetNode(context.Background(), ypath.Path("//home/cdc/test/generic/temp/test_table/@test"), &data, nil))
	require.Equal(t, true, data)
	teardown(ytClient, ypath.Path(cfg.Path()))
}

func TestIncludeTimeoutAttributeSingleStaticTable(t *testing.T) {
	if recipe.TestContainerEnabled() {
		t.Skip()
	}
	schema_ := abstract.NewTableSchema([]abstract.ColSchema{
		{
			DataType:   "double",
			ColumnName: "test",
			PrimaryKey: true,
		},
	})
	cfg := yt2.NewYtDestinationV1(yt2.YtDestination{
		Atomicity:     yt.AtomicityFull,
		CellBundle:    "default",
		PrimaryMedium: "default",
		CustomAttributes: map[string]string{
			"expiration_timeout": "604800000",
			"expiration_time":    "\"2200-01-12T03:32:51.298047Z\"",
		},
		Path:                     "//home/cdc/test/generic/temp",
		UseStaticTableOnSnapshot: true,
		Cluster:                  os.Getenv("YT_PROXY")},
	)
	cfg.WithDefaults()
	table, err := newSinker(cfg, "some_uniq_transfer_id", 0, logger.Log, metrics.NewRegistry(), coordinator.NewFakeClient())
	require.NoError(t, err)
	require.NoError(t, table.Push([]abstract.ChangeItem{{
		TableSchema: schema_,
		Kind:        abstract.InitShardedTableLoad,
		Table:       "test_timeout_table",
	}}))
	require.NoError(t, table.Push([]abstract.ChangeItem{{
		TableSchema: schema_,
		Kind:        abstract.InitTableLoad,
		Table:       "test_timeout_table",
	}}))
	require.NoError(t, table.Push([]abstract.ChangeItem{
		{
			TableSchema:  schema_,
			Kind:         abstract.InsertKind,
			ColumnNames:  []string{"test"},
			ColumnValues: []interface{}{3.99},
			Table:        "test_timeout_table",
		},
	}))
	require.NoError(t, table.Push([]abstract.ChangeItem{{
		TableSchema: schema_,
		Kind:        abstract.DoneTableLoad,
		Table:       "test_timeout_table",
	}}))
	require.NoError(t, table.Push([]abstract.ChangeItem{{
		TableSchema: schema_,
		Kind:        abstract.DoneShardedTableLoad,
		Table:       "test_timeout_table",
	}}))

	ytClient, err := ytclient.FromConnParams(cfg, logger.Log)
	require.NoError(t, err)
	defer teardown(ytClient, ypath.Path(cfg.Path()))

	var timeout int64
	require.NoError(t, ytClient.GetNode(context.Background(), ypath.Path("//home/cdc/test/generic/temp/test_timeout_table/@expiration_timeout"), &timeout, nil))
	require.Equal(t, int64(604800000), timeout)
	var expTime string
	require.NoError(t, ytClient.GetNode(context.Background(), ypath.Path("//home/cdc/test/generic/temp/test_timeout_table/@expiration_time"), &expTime, nil))
	require.Equal(t, "2200-01-12T03:32:51.298047Z", expTime)
}
