package staticsink

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var bigRowSchema = abstract.NewTableSchema([]abstract.ColSchema{
	{DataType: schema.TypeInt8.String(), ColumnName: "MyInt8", PrimaryKey: false},
	{DataType: schema.TypeInt16.String(), ColumnName: "MyInt16", PrimaryKey: false},
	{DataType: schema.TypeInt32.String(), ColumnName: "MyInt32", PrimaryKey: false},
	{DataType: schema.TypeInt64.String(), ColumnName: "MyInt64", PrimaryKey: true},
	{DataType: schema.TypeUint8.String(), ColumnName: "MyUint8", PrimaryKey: false},
	{DataType: schema.TypeUint16.String(), ColumnName: "MyUint16", PrimaryKey: false},
	{DataType: schema.TypeUint32.String(), ColumnName: "MyUint32", PrimaryKey: false},
	{DataType: schema.TypeUint64.String(), ColumnName: "MyUint64", PrimaryKey: false},
	{DataType: schema.TypeFloat32.String(), ColumnName: "MyFloat", PrimaryKey: false},
	{DataType: schema.TypeFloat64.String(), ColumnName: "MyDouble", PrimaryKey: false},
	{DataType: schema.TypeBytes.String(), ColumnName: "MyBytes", PrimaryKey: false},
	{DataType: schema.TypeString.String(), ColumnName: "MyString", PrimaryKey: false},
	{DataType: schema.TypeBoolean.String(), ColumnName: "MyBoolean", PrimaryKey: false},
	{DataType: schema.TypeAny.String(), ColumnName: "MyAny", PrimaryKey: false},
})

type bigRow struct {
	MyInt8    int8        `yson:"MyInt8"`
	MyInt16   int16       `yson:"MyInt16"`
	MyInt32   int32       `yson:"MyInt32"`
	MyInt64   int64       `yson:"MyInt64"`
	MyUint8   uint8       `yson:"MyUint8"`
	MyUint16  uint16      `yson:"MyUint16"`
	MyUint32  uint32      `yson:"MyUint32"`
	MyUint64  uint64      `yson:"MyUint64"`
	MyFloat   float32     `yson:"MyFloat"`
	MyDouble  float64     `yson:"MyDouble"`
	MyBytes   []byte      `yson:"MyBytes"`
	MyString  string      `yson:"MyString"`
	MyBoolean bool        `yson:"MyBoolean"`
	MyAny     interface{} `yson:"MyAny"`
}

func newBigRow() bigRow {
	var f bigRow
	_ = gofakeit.Struct(&f)
	return f
}

func (b *bigRow) toValues() []interface{} {
	return []interface{}{
		b.MyInt8,
		b.MyInt16,
		b.MyInt32,
		b.MyInt64,
		b.MyUint8,
		b.MyUint16,
		b.MyUint32,
		b.MyUint64,
		b.MyFloat,
		b.MyDouble,
		b.MyBytes,
		b.MyString,
		b.MyBoolean,
		b.MyAny,
	}
}

func (b *bigRow) toChangeItem(namespace, name string) abstract.ChangeItem {
	return abstract.ChangeItem{
		TableSchema:  bigRowSchema,
		Kind:         abstract.InsertKind,
		Schema:       namespace,
		Table:        name,
		ColumnNames:  bigRowSchema.Columns().ColumnNames(),
		ColumnValues: b.toValues(),
	}
}

func TestStaticSink(t *testing.T) {
	t.Run("simple test", staticTableSimple)
	t.Run("wrong schema test", wrongOrderOfValuesInChangeItem)
	t.Run("custom attributes test", customAttributesStaticTable)
	t.Run("timeout attribute test", includeTimeoutAttributeStaticTable)
}

func staticTableSimple(t *testing.T) {
	var err error
	path := ypath.Path("//home/cdc/test/TM-3788/staticTableSimple")
	// create single static table for change item consumption
	env, cfg, ytCancel := initYt(t, path.String())
	cp := coordinator.NewStatefulFakeClient()
	defer teardown(env, path)
	defer ytCancel()
	// schema might be unknown during initialization
	tableID := abstract.TableID{
		Namespace: "ns",
		Name:      "weird_table",
	}
	statTable, err := NewStaticSink(cfg, cp, "dtt", metrics.NewRegistry(), logger.Log)
	require.NoError(t, err)

	// generate some amount of random change items
	var data []bigRow
	var items []abstract.ChangeItem
	for i := 0; i < 79; i++ {
		row := newBigRow()
		data = append(data, row)
		items = append(items, row.toChangeItem(tableID.Namespace, tableID.Name))
	}
	// push initial items
	err = statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitShardedTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}})
	require.NoError(t, err)
	err = statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}})
	require.NoError(t, err)
	// write change items
	err = statTable.Push(items)
	require.NoError(t, err)
	// push final items
	err = statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.DoneTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}})
	require.NoError(t, err)
	err = statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.DoneShardedTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}})
	require.NoError(t, err)

	completable, ok := statTable.(abstract.Committable)
	require.True(t, ok)
	require.NoError(t, completable.Commit())
	require.NoError(t, statTable.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// check what nodes do we have
	var listNodeResult []struct {
		Name string `yson:",value"`
	}
	directoryNode := ypath.Path(cfg.Path())
	err = env.YT.ListNode(ctx, directoryNode, &listNodeResult, nil)
	logger.Log.Info("List of table in destination folder", log.Any("list", listNodeResult))
	require.NoError(t, err)
	require.Len(t, listNodeResult, 1, "there should be only one child")

	tableNode := yt2.SafeChild(directoryNode, listNodeResult[0].Name)
	// load result from YT
	rows, err := env.YT.ReadTable(ctx, tableNode.YPath(), nil)
	require.NoError(t, err)
	var res []bigRow
	for rows.Next() {
		var row bigRow
		require.NoError(t, rows.Scan(&row))
		res = append(res, row)
	}
	// sort answer to preserve order
	sort.Slice(data, func(i, j int) bool {
		return data[i].MyInt64 < data[j].MyInt64
	})
	sort.Slice(res, func(i, j int) bool {
		return res[i].MyInt64 < res[j].MyInt64
	})
	require.Equal(t, data, res)
}

func wrongOrderOfValuesInChangeItem(t *testing.T) {
	var err error
	path := ypath.Path("//home/cdc/test/TM-3788/wrongOrderOfValuesInChangeItem")
	// create single static table for change item consumption
	env, cfg, ytCancel := initYt(t, path.String())
	cp := coordinator.NewStatefulFakeClient()
	defer teardown(env, path)
	defer ytCancel()
	// schema might be unknown during initialization
	tableID := abstract.TableID{
		Namespace: "ns",
		Name:      "weird_table_2",
	}
	statTable, err := NewStaticSink(cfg, cp, "dtt", metrics.NewRegistry(), logger.Log)
	defer require.NoError(t, statTable.Close())
	require.NoError(t, err)

	// push initial item
	err = statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitShardedTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}})
	require.NoError(t, err)
	err = statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}})
	require.NoError(t, err)
	// write wrong change item (not compliant to scheme)
	row := newBigRow()
	values := row.toValues()
	values[3] = false
	err = statTable.Push([]abstract.ChangeItem{
		{
			TableSchema:  bigRowSchema,
			Kind:         abstract.InsertKind,
			Schema:       tableID.Namespace,
			Table:        tableID.Name,
			ColumnNames:  bigRowSchema.Columns().ColumnNames(),
			ColumnValues: values,
		}})
	require.ErrorContains(t, err, "unaccepted value false for yt type int64")
	err = statTable.Push([]abstract.ChangeItem{
		{
			TableSchema: bigRowSchema,
			Kind:        abstract.DoneTableLoad,
			Schema:      tableID.Namespace,
			Table:       tableID.Name,
		}})
	require.NoError(t, err)
	require.NoError(t, statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.DoneShardedTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}}))
}

func customAttributesStaticTable(t *testing.T) {
	path := ypath.Path("//home/cdc/test/static/test_table")
	// create single static table for change item consumption
	env, cfg, ytCancel := initYt(t, path.String())
	cp := coordinator.NewStatefulFakeClient()
	defer teardown(env, path)
	defer ytCancel()
	// schema might be unknown during initialization
	tableID := abstract.TableID{
		Namespace: "ns",
		Name:      "weird_table_2",
	}

	statTable, err := NewStaticSink(cfg, cp, "dtt", metrics.NewRegistry(), logger.Log)
	require.NoError(t, err)
	// generate some amount of random change items
	var items []abstract.ChangeItem
	for i := 0; i < 1; i++ {
		row := newBigRow()
		items = append(items, row.toChangeItem(tableID.Namespace, tableID.Name))
	}
	// push initial items
	require.NoError(t, statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitShardedTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}}))
	require.NoError(t, statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}}))
	// write change items
	require.NoError(t, statTable.Push(items))
	// push final items
	require.NoError(t, statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.DoneTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}}))
	require.NoError(t, statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.DoneShardedTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}}))

	completable, ok := statTable.(abstract.Committable)
	require.True(t, ok)
	require.NoError(t, completable.Commit())

	var attr bool
	require.NoError(t, env.YT.GetNode(env.Ctx, ypath.Path("//home/cdc/test/static/test_table/ns_weird_table_2").Attr("test"), &attr, nil))
	require.Equal(t, true, attr)
}

func includeTimeoutAttributeStaticTable(t *testing.T) {
	path := ypath.Path("//home/cdc/test/TM-8315/TimeoutAttributeStaticTable")
	// create single static table for change item consumption
	env, cfg, ytCancel := initYt(t, path.String())
	cp := coordinator.NewStatefulFakeClient()
	defer teardown(env, path)
	defer ytCancel()
	// schema might be unknown during initialization
	tableID := abstract.TableID{
		Namespace: "ns",
		Name:      "weird_table_2",
	}

	statTable, err := NewStaticSink(cfg, cp, "dtt", metrics.NewRegistry(), logger.Log)
	require.NoError(t, err)
	// generate some amount of random change items
	var items []abstract.ChangeItem
	for i := 0; i < 1; i++ {
		row := newBigRow()
		items = append(items, row.toChangeItem(tableID.Namespace, tableID.Name))
	}
	// push initial items
	require.NoError(t, statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitShardedTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}}))
	require.NoError(t, statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.InitTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}}))
	// write change items
	require.NoError(t, statTable.Push(items))
	// push final items
	require.NoError(t, statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.DoneTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}}))
	require.NoError(t, statTable.Push([]abstract.ChangeItem{{
		TableSchema: bigRowSchema,
		Kind:        abstract.DoneShardedTableLoad,
		Schema:      tableID.Namespace,
		Table:       tableID.Name,
	}}))

	completable, ok := statTable.(abstract.Committable)
	require.True(t, ok)
	require.NoError(t, completable.Commit())

	var timeout int64
	require.NoError(t, env.YT.GetNode(env.Ctx, ypath.Path("//home/cdc/test/TM-8315/TimeoutAttributeStaticTable/ns_weird_table_2").Attr("expiration_timeout"), &timeout, nil))
	require.Equal(t, int64(604800000), timeout)
	var expTime string
	require.NoError(t, env.YT.GetNode(env.Ctx, ypath.Path("//home/cdc/test/TM-8315/TimeoutAttributeStaticTable/ns_weird_table_2").Attr("expiration_time"), &expTime, nil))
	require.Equal(t, "2200-01-12T03:32:51.298047Z", expTime)
}

func initYt(t *testing.T, path string) (testEnv *yttest.Env, testCfg yt2.YtDestinationModel, testTeardown func()) {
	env, cancel := recipe.NewEnv(t)
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
		Static: true,
	})
	cfg.WithDefaults()
	return env, cfg, func() {
		cancel()
	}
}
func teardown(env *yttest.Env, path ypath.Path) {
	err := env.YT.RemoveNode(
		env.Ctx,
		path,
		&yt.RemoveNodeOptions{
			Recursive: true,
			Force:     true,
		},
	)
	if err != nil {
		logger.Log.Error("unable to delete test folder", log.Error(err))
	}
}
