package statictable

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	"github.com/transferia/transferia/pkg/providers/yt/sink"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	simpleSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{DataType: "int32", ColumnName: "key"},
		{DataType: "any", ColumnName: "val"},
	})
	sortedTableSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{DataType: "int32", ColumnName: "key", PrimaryKey: true},
		{DataType: "any", ColumnName: "val"},
	})
	extendedTableSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{DataType: "int32", ColumnName: "key"},
		{DataType: "any", ColumnName: "val"},
		{DataType: "string", ColumnName: "name"},
	})
	primaryKeysSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{DataType: "int32", ColumnName: "key", PrimaryKey: true},
		{DataType: "any", ColumnName: "val", PrimaryKey: true},
	})

	mainTxTimeout = yson.Duration(5 * time.Minute)
)

type executor struct {
	t          *testing.T
	MainTxID   yt.TxID
	TransferID string
	Client     yt.Client
	Cfg        yt2.YtDestinationModel
	Metrics    *stats.SinkerStats
}

func (e executor) init(path ypath.Path, schema *abstract.TableSchema) {
	require.NoError(e.t, Init(e.Client, &InitOptions{
		MainTxID:         e.MainTxID,
		TransferID:       e.TransferID,
		Schema:           schema.Columns(),
		Path:             path,
		OptimizeFor:      e.Cfg.OptimizeFor(),
		CustomAttributes: e.Cfg.CustomAttributes(),
		Logger:           logger.Log,
	}))
}

func (e executor) write(path ypath.Path, input ...[]abstract.ChangeItem) error {
	ctx := context.Background()
	txClient, err := e.Client.BeginTx(ctx, &yt.StartTxOptions{
		TransactionOptions: &yt.TransactionOptions{TransactionID: e.MainTxID},
	})
	require.NoError(e.t, err)

	wr, err := NewWriter(WriterConfig{
		TransferID:  e.TransferID,
		TxClient:    txClient,
		Path:        path,
		Spec:        e.Cfg.Spec().GetConfig(),
		ChunkSize:   1024 * 1024,
		Logger:      logger.Log,
		Metrics:     e.Metrics,
		StringLimit: sink.YtStatMaxStringLength,
	})
	require.NoError(e.t, err)

	for _, in := range input {
		if err := wr.Write(in); err != nil {
			_ = txClient.Abort()
			return err
		}
	}

	if err := wr.Commit(); err != nil {
		_ = txClient.Abort()
		return err
	}
	require.NoError(e.t, txClient.Commit())

	return nil
}

func (e executor) commit(path ypath.Path, schema *abstract.TableSchema, cleanupType model.CleanupType, allowSorting bool) {
	require.NoError(e.t, Commit(e.Client, &CommitOptions{
		MainTxID:         e.MainTxID,
		TransferID:       e.TransferID,
		Schema:           schema.Columns(),
		Path:             path,
		CleanupType:      cleanupType,
		AllowedSorting:   allowSorting,
		Pool:             e.Cfg.Pool(),
		OptimizeFor:      e.Cfg.OptimizeFor(),
		CustomAttributes: e.Cfg.CustomAttributes(),
		Logger:           logger.Log,
	}))
	require.NoError(e.t, e.Client.CommitTx(context.Background(), e.MainTxID, nil))
}

func newExecutor(t *testing.T, client yt.Client, cfg yt2.YtDestinationModel) executor {
	txID, err := client.StartTx(context.Background(), &yt.StartTxOptions{Timeout: &mainTxTimeout})
	require.NoError(t, err)
	return executor{
		t:          t,
		MainTxID:   txID,
		TransferID: "dtt",
		Client:     client,
		Cfg:        cfg,
		Metrics:    stats.NewSinkerStats(metrics.NewRegistry()),
	}
}

type tableStruct struct {
	Key  int32  `yson:"key"`
	Val  string `yson:"val"`
	Name string `yson:"name"`
}

func TestStaticTable(t *testing.T) {
	t.Run("Simple", simple)
	t.Run("Sorted", sorted)
	t.Run("ExtendedSchema", extendedSchema)
	t.Run("Parallel", parallel)
	t.Run("WrongChangeItems", wrongChangeItems)
	t.Run("ErrorWritingChunk", errorChunkWriting)
	t.Run("SchemaWithOnlyPrimaryKeys", schemaWithOnlyPrimaryKeys)
}

func simple(t *testing.T) {
	path := ypath.Path("//home/cdc/test/TM-7192")
	env, cfg, ytCancel := initYt(t, path.String())
	defer ytCancel()
	defer teardown(env, path)

	tableName := "simple_test"
	tableYTPath := yt2.SafeChild(ypath.Path(cfg.Path()), tableName)

	// write unsorted static table in two stages
	st := newExecutor(t, env.YT, cfg)
	st.init(tableYTPath, simpleSchema)

	err := st.write(tableYTPath,
		makeChangeItems(simpleSchema, []string{"key", "val"}, [][]interface{}{
			{int32(1), "some"},
			{int32(2), "body"},
		}),
		makeChangeItems(simpleSchema, []string{"key", "val"}, [][]interface{}{
			{int32(3), "once"},
			{int32(4), "told"},
		}))
	require.NoError(t, err)

	st.commit(tableYTPath, simpleSchema, model.Drop, false)

	checkTmpTables(t, env, tableYTPath)
	checkResult(t, env, tableYTPath, []tableStruct{
		{Key: int32(1), Val: "some"},
		{Key: int32(2), Val: "body"},
		{Key: int32(3), Val: "once"},
		{Key: int32(4), Val: "told"},
	}, true)

	// append change items with DisabledCleanup
	st = newExecutor(t, env.YT, cfg)
	st.init(tableYTPath, simpleSchema)

	err = st.write(tableYTPath, makeChangeItems(simpleSchema, []string{"key", "val"}, [][]interface{}{
		{int32(5), "me"},
		{int32(6), "the"},
		{int32(7), "world"},
	}))
	require.NoError(t, err)

	st.commit(tableYTPath, simpleSchema, model.DisabledCleanup, false)

	checkTmpTables(t, env, tableYTPath)
	checkResult(t, env, tableYTPath, []tableStruct{
		{Key: int32(1), Val: "some"},
		{Key: int32(2), Val: "body"},
		{Key: int32(3), Val: "once"},
		{Key: int32(4), Val: "told"},
		{Key: int32(5), Val: "me"},
		{Key: int32(6), Val: "the"},
		{Key: int32(7), Val: "world"},
	}, true)

	// overwrite an existing table with CleanupType: Drop
	st = newExecutor(t, env.YT, cfg)
	st.init(tableYTPath, simpleSchema)

	err = st.write(tableYTPath, makeChangeItems(simpleSchema, []string{"key", "val"}, [][]interface{}{
		{int32(1), "welcome"},
		{int32(3), "to"},
		{int32(5), "the"},
		{int32(7), "club"},
	}))
	require.NoError(t, err)

	st.commit(tableYTPath, simpleSchema, model.Drop, false)

	checkTmpTables(t, env, tableYTPath)
	checkResult(t, env, tableYTPath, []tableStruct{
		{Key: int32(1), Val: "welcome"},
		{Key: int32(3), Val: "to"},
		{Key: int32(5), Val: "the"},
		{Key: int32(7), Val: "club"},
	}, true)
}

func sorted(t *testing.T) {
	path := ypath.Path("//home/cdc/test/TM-7192")
	env, cfg, ytCancel := initYt(t, path.String())
	defer ytCancel()
	defer teardown(env, path)

	tableName := "sorted_test"
	tableYTPath := yt2.SafeChild(ypath.Path(cfg.Path()), tableName)
	// sorted table
	st := newExecutor(t, env.YT, cfg)
	st.init(tableYTPath, sortedTableSchema)

	err := st.write(tableYTPath, makeChangeItems(sortedTableSchema, []string{"key", "val"}, [][]interface{}{
		{int32(3), "take"},
		{int32(1), "I'm"},
	}), makeChangeItems(sortedTableSchema, []string{"key", "val"}, [][]interface{}{
		{int32(4), "my"},
		{int32(2), "gonna"},
	}))
	require.NoError(t, err)

	st.commit(tableYTPath, sortedTableSchema, model.Drop, true)

	checkTmpTables(t, env, tableYTPath)
	checkResult(t, env, tableYTPath, []tableStruct{
		{Key: int32(1), Val: "I'm"},
		{Key: int32(2), Val: "gonna"},
		{Key: int32(3), Val: "take"},
		{Key: int32(4), Val: "my"},
	}, false)

	// append items to sorted table

	st = newExecutor(t, env.YT, cfg)
	st.init(tableYTPath, sortedTableSchema)

	err = st.write(tableYTPath, makeChangeItems(sortedTableSchema, []string{"key", "val"}, [][]interface{}{
		{int32(5), "horse"},
		{int32(7), "the"},
		{int32(6), "to"},
	}))
	require.NoError(t, err)

	st.commit(tableYTPath, sortedTableSchema, model.DisabledCleanup, true)

	checkTmpTables(t, env, tableYTPath)
	checkResult(t, env, tableYTPath, []tableStruct{
		{Key: int32(1), Val: "I'm"},
		{Key: int32(2), Val: "gonna"},
		{Key: int32(3), Val: "take"},
		{Key: int32(4), Val: "my"},
		{Key: int32(5), Val: "horse"},
		{Key: int32(6), Val: "to"},
		{Key: int32(7), Val: "the"},
	}, false)
}

func extendedSchema(t *testing.T) {
	path := ypath.Path("//home/cdc/test/TM-7192")
	env, cfg, ytCancel := initYt(t, path.String())
	defer ytCancel()
	defer teardown(env, path)

	tableName := "arc_warden_extended_test"
	tableYTPath := yt2.SafeChild(ypath.Path(cfg.Path()), tableName)

	// write table with simple schema
	st := newExecutor(t, env.YT, cfg)
	st.init(tableYTPath, simpleSchema)

	err := st.write(tableYTPath, makeChangeItems(simpleSchema, []string{"key", "val"}, [][]interface{}{
		{int32(1), "some"},
		{int32(2), "body"},
		{int32(3), "once"},
		{int32(4), "told"},
	}))
	require.NoError(t, err)

	st.commit(tableYTPath, simpleSchema, model.DisabledCleanup, false)

	checkTmpTables(t, env, tableYTPath)
	checkResult(t, env, tableYTPath, []tableStruct{
		{Key: int32(1), Val: "some"},
		{Key: int32(2), Val: "body"},
		{Key: int32(3), Val: "once"},
		{Key: int32(4), Val: "told"},
	}, true)

	// append items with extended schema

	st = newExecutor(t, env.YT, cfg)
	st.init(tableYTPath, extendedTableSchema)

	err = st.write(tableYTPath, makeChangeItems(extendedTableSchema, []string{"key", "val", "name"}, [][]interface{}{
		{int32(5), "me", "is"},
		{int32(6), "the", "gonna"},
		{int32(7), "world", "roll"},
	}))
	require.NoError(t, err)

	st.commit(tableYTPath, extendedTableSchema, model.DisabledCleanup, false)

	checkTmpTables(t, env, tableYTPath)
	checkResult(t, env, tableYTPath, []tableStruct{
		{Key: int32(1), Val: "some"},
		{Key: int32(2), Val: "body"},
		{Key: int32(3), Val: "once"},
		{Key: int32(4), Val: "told"},
		{Key: int32(5), Val: "me", Name: "is"},
		{Key: int32(6), Val: "the", Name: "gonna"},
		{Key: int32(7), Val: "world", Name: "roll"},
	}, true)
}

func parallel(t *testing.T) {
	path := ypath.Path("//home/cdc/test/TM-7192")
	env, cfg, ytCancel := initYt(t, path.String())
	defer ytCancel()
	defer teardown(env, path)

	tableName := "arc_warden_parallel_test"
	tableYTPath := yt2.SafeChild(ypath.Path(cfg.Path()), tableName)

	// write unsorted static table parallel
	st := newExecutor(t, env.YT, cfg)
	st.init(tableYTPath, simpleSchema)

	items := [][]abstract.ChangeItem{
		makeChangeItems(simpleSchema, []string{"key", "val"}, [][]interface{}{
			{int32(1), "some"},
			{int32(2), "body"},
		}),
		makeChangeItems(simpleSchema, []string{"key", "val"}, [][]interface{}{
			{int32(3), "once"},
			{int32(4), "told"},
		}),
		makeChangeItems(simpleSchema, []string{"key", "val"}, [][]interface{}{
			{int32(5), "the"},
			{int32(6), "world"},
		}),
		makeChangeItems(simpleSchema, []string{"key", "val"}, [][]interface{}{
			{int32(7), "is"},
			{int32(8), "gonna"},
		}),
		makeChangeItems(simpleSchema, []string{"key", "val"}, [][]interface{}{
			{int32(9), "roll"},
			{int32(10), "me"},
		}),
	}

	errCh := make(chan error)
	for _, batch := range items {
		go func(ch chan<- error, input []abstract.ChangeItem) {
			ch <- st.write(tableYTPath, input)
		}(errCh, batch)
	}

	var err error
	for i := 0; i < len(items); i++ {
		err = <-errCh
		require.NoError(t, err)
	}

	st.commit(tableYTPath, simpleSchema, model.DisabledCleanup, false)

	checkTmpTables(t, env, tableYTPath)
	checkResult(t, env, tableYTPath, []tableStruct{
		{Key: int32(1), Val: "some"},
		{Key: int32(2), Val: "body"},
		{Key: int32(3), Val: "once"},
		{Key: int32(4), Val: "told"},
		{Key: int32(5), Val: "the"},
		{Key: int32(6), Val: "world"},
		{Key: int32(7), Val: "is"},
		{Key: int32(8), Val: "gonna"},
		{Key: int32(9), Val: "roll"},
		{Key: int32(10), Val: "me"},
	}, true)
}

func wrongChangeItems(t *testing.T) {
	path := ypath.Path("//home/cdc/test/TM-7192")
	env, cfg, ytCancel := initYt(t, path.String())
	defer ytCancel()
	defer teardown(env, path)

	tableName := "arc_warden_wrong_items_test"
	tableYTPath := yt2.SafeChild(ypath.Path(cfg.Path()), tableName)

	// write unsorted static table with wrong ChangeItems
	st := newExecutor(t, env.YT, cfg)
	st.init(tableYTPath, simpleSchema)

	err := st.write(tableYTPath, makeChangeItems(simpleSchema, []string{"key", "val"}, [][]interface{}{
		{int32(1), "some"},
		{int32(2), "body"},
	}))
	require.NoError(t, err)

	err = st.write(tableYTPath, []abstract.ChangeItem{
		{
			TableSchema: simpleSchema,
			Kind:        abstract.DeleteKind,
			Schema:      "sch",
			Table:       "test",
		},
		{
			TableSchema: simpleSchema,
			Kind:        abstract.UpdateKind,
			Schema:      "sch",
			Table:       "test",
		},
	})
	require.Error(t, err)

	st.commit(tableYTPath, simpleSchema, model.Drop, false)

	checkTmpTables(t, env, tableYTPath)
	checkResult(t, env, tableYTPath, []tableStruct{
		{Key: int32(1), Val: "some"},
		{Key: int32(2), Val: "body"},
	}, true)
}

func errorChunkWriting(t *testing.T) {
	path := ypath.Path("//home/cdc/test/TM-7192")
	env, cfg, ytCancel := initYt(t, path.String())
	defer ytCancel()
	defer teardown(env, path)

	tableName := "arc_warden_error_writing_test"
	tableYTPath := yt2.SafeChild(ypath.Path(cfg.Path()), tableName)

	// write unsorted static table with error writing chunk
	st := newExecutor(t, env.YT, cfg)
	st.init(tableYTPath, simpleSchema)

	err := st.write(tableYTPath, makeChangeItems(simpleSchema, []string{"key", "val"}, [][]interface{}{
		{int32(1), "some"},
		{int32(2), "body"},
	}))
	require.NoError(t, err)

	err = st.write(tableYTPath, makeChangeItems(simpleSchema, []string{"key", "val"}, [][]interface{}{
		{int32(3), "once"},
		{"told", "me"},
	}))
	require.Error(t, err)

	st.commit(tableYTPath, simpleSchema, model.Drop, false)

	checkTmpTables(t, env, tableYTPath)
	checkResult(t, env, tableYTPath, []tableStruct{
		{Key: int32(1), Val: "some"},
		{Key: int32(2), Val: "body"},
	}, true)
}

func schemaWithOnlyPrimaryKeys(t *testing.T) {
	path := ypath.Path("//home/cdc/test/TM-7192")
	env, cfg, ytCancel := initYt(t, path.String())
	defer ytCancel()
	defer teardown(env, path)

	tableName := "arc_warden_primary_keys_test"
	tableYTPath := yt2.SafeChild(ypath.Path(cfg.Path()), tableName)

	// write sorted static table with only primary keys
	st := newExecutor(t, env.YT, cfg)
	st.init(tableYTPath, primaryKeysSchema)

	err := st.write(tableYTPath, makeChangeItems(primaryKeysSchema, []string{"key", "val"}, [][]interface{}{
		{int32(1), "a"},
		{int32(2), "b"},
		{int32(3), "c"},
		{int32(4), "d"},
	}))
	require.NoError(t, err)

	st.commit(tableYTPath, primaryKeysSchema, model.Drop, false)

	checkTmpTables(t, env, tableYTPath)
	checkResult(t, env, tableYTPath, []tableStruct{
		{Key: int32(1), Val: "a"},
		{Key: int32(2), Val: "b"},
		{Key: int32(3), Val: "c"},
		{Key: int32(4), Val: "d"},
	}, true)
}

func checkTmpTables(t *testing.T, env *yttest.Env, path ypath.Path) {
	ok, err := env.YT.NodeExists(context.Background(), makeTablePath(path, "dtt", tmpNamePostfix), nil)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = env.YT.NodeExists(context.Background(), makeTablePath(path, "dtt", sortedNamePostfix), nil)
	require.NoError(t, err)
	require.False(t, ok)
}

func checkResult(t *testing.T, env *yttest.Env, tablePath ypath.Path, expectedResult []tableStruct, needSort bool) {
	rows, err := env.YT.ReadTable(context.Background(), tablePath, nil)
	require.NoError(t, err)
	var res []tableStruct
	for rows.Next() {
		var row tableStruct
		require.NoError(t, rows.Scan(&row))
		res = append(res, row)
	}

	if needSort {
		sort.Slice(res, func(i, j int) bool {
			return res[i].Key < res[j].Key
		})
	}
	require.Equal(t, len(expectedResult), len(res))
	for i := range expectedResult {
		require.Equal(t, expectedResult[i].Key, res[i].Key)
		require.Equal(t, expectedResult[i].Val, res[i].Val)
		require.Equal(t, expectedResult[i].Name, res[i].Name)
	}

	checkTableAttrs(t, env, tablePath, !needSort)
}

func checkTableAttrs(t *testing.T, env *yttest.Env, tablePath ypath.Path, expectedSorted bool) {
	var sorted bool
	require.NoError(t, env.YT.GetNode(context.Background(), tablePath.Attr("sorted"), &sorted, nil))
	require.Equal(t, expectedSorted, sorted)

	var dynamic bool
	require.NoError(t, env.YT.GetNode(context.Background(), tablePath.Attr("dynamic"), &dynamic, nil))
	require.False(t, dynamic)
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
			"test": "%true",
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

func makeChangeItems(schema *abstract.TableSchema, names []string, values [][]interface{}) []abstract.ChangeItem {
	var items []abstract.ChangeItem
	for _, v := range values {
		items = append(items, abstract.ChangeItem{
			TableSchema:  schema,
			Kind:         abstract.InsertKind,
			Schema:       "sch",
			Table:        "test",
			ColumnNames:  names,
			ColumnValues: v,
		})
	}
	return items
}
