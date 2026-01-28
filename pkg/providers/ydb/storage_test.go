package ydb

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/util"
	ydbrecipe "github.com/transferia/transferia/tests/helpers/ydb_recipe"
	ydbtable "github.com/transferia/transferia/tests/helpers/ydb_recipe/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	demoSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "_timestamp", DataType: "DateTime", PrimaryKey: true},
		{ColumnName: "_partition", DataType: string(schema.TypeString), PrimaryKey: true},
		{ColumnName: "_offset", DataType: string(schema.TypeInt64), PrimaryKey: true},
		{ColumnName: "_idx", DataType: string(schema.TypeInt32), PrimaryKey: true},
		{ColumnName: "_rest", DataType: string(schema.TypeAny)},
		{ColumnName: "raw_value", DataType: string(schema.TypeString)},
	})
	rows = []map[string]interface{}{
		{
			"_timestamp": time.Now(),
			"_partition": "test",
			"_offset":    321,
			"_idx":       0,
			"_rest": map[string]interface{}{
				"some_child": 321,
			},
			"raw_value": "some_child is 321",
		},
	}
)

func TestYdbStorage_TableLoad(t *testing.T) {
	endpoint, port, database, _ := ydbrecipe.InstancePortDatabaseCreds(t)
	instance := fmt.Sprintf("%s:%d", endpoint, port)
	token := ydbrecipe.Token()

	src := &YdbSource{
		Token:    model.SecretString(token),
		Database: database,
		Instance: instance,
		Tables:   nil,
		TableColumnsFilter: []YdbColumnsFilter{{
			TableNamesRegexp:  "^foo_t_.*",
			ColumnNamesRegexp: "raw_value",
			Type:              YdbColumnsBlackList,
		}},
		SubNetworkID:     "",
		Underlay:         false,
		ServiceAccountID: "",
	}

	st, err := NewStorage(src.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))

	require.NoError(t, err)

	cfg := YdbDestination{
		Database: database,
		Token:    model.SecretString(token),
		Instance: instance,
	}
	cfg.WithDefaults()
	sinker, err := NewSinker(logger.Log, &cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	var names []string
	var vals []interface{}
	for _, row := range rows {
		for k, v := range row {
			names = append(names, k)
			vals = append(vals, v)
		}
	}

	require.NoError(t, sinker.Push([]abstract.ChangeItem{{
		Kind:         "insert",
		Schema:       "foo",
		Table:        "t_5",
		ColumnNames:  names,
		ColumnValues: vals,
		TableSchema:  demoSchema,
	}}))

	upCtx := util.ContextWithTimestamp(context.Background(), time.Now())
	var result []abstract.ChangeItem

	err = st.LoadTable(upCtx, abstract.TableDescription{Schema: "", Name: "foo_t_5"}, func(input []abstract.ChangeItem) error {
		for _, row := range input {
			if row.IsRowEvent() {
				result = append(result, row)
			}
		}
		return nil
	})

	require.NoError(t, err)
	require.NotContainsf(t, "raw_value", result[0].ColumnNames, "filtered column presents in result")
	require.Equal(t, len(rows), len(result), "not all rows are loaded")
}

func TestYdbStorage_TableList(t *testing.T) {
	endpoint, port, database, _ := ydbrecipe.InstancePortDatabaseCreds(t)
	instance := fmt.Sprintf("%s:%d", endpoint, port)
	token := ydbrecipe.Token()

	src := YdbSource{
		Token:              model.SecretString(token),
		Database:           database,
		Instance:           instance,
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}

	st, err := NewStorage(src.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))

	require.NoError(t, err)

	cfg := YdbDestination{
		Database: database,
		Token:    model.SecretString(token),
		Instance: instance,
	}
	cfg.WithDefaults()
	sinker, err := NewSinker(logger.Log, &cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	var names []string
	var vals []interface{}
	for _, row := range rows {
		for k, v := range row {
			names = append(names, k)
			vals = append(vals, v)
		}
	}
	for i := 0; i < 6; i++ {
		require.NoError(t, sinker.Push([]abstract.ChangeItem{{
			Kind:         "insert",
			Schema:       "table_list",
			Table:        fmt.Sprintf("t_%v", i),
			ColumnNames:  names,
			ColumnValues: vals,
			TableSchema:  demoSchema,
		}}))
	}
	require.NoError(t, err)
	tables, err := st.TableList(nil)
	require.NoError(t, err)
	for t := range tables {
		logger.Log.Infof("input table: %v %v", t.Namespace, t.Name)
	}

	tableForTest := 0
	for currTable := range tables {
		if len(currTable.Name) > 10 && currTable.Name[:10] == "table_list" {
			tableForTest++
		}
	}

	require.Equal(t, 6, tableForTest)

	upCtx := util.ContextWithTimestamp(context.Background(), time.Now())

	err = st.LoadTable(upCtx, abstract.TableDescription{Schema: "", Name: "foo_t_5"}, func(input []abstract.ChangeItem) error {
		abstract.Dump(input)
		return nil
	})
	require.NoError(t, err)
}

func TestYdbStorage_MaxBatchLenAndSize(t *testing.T) {
	endpoint, port, database, _ := ydbrecipe.InstancePortDatabaseCreds(t)
	instance := fmt.Sprintf("%s:%d", endpoint, port)
	token := ydbrecipe.Token()

	testTableName := "max_read_test_table"
	prepareTestTable(t, path.Join(database, testTableName))

	cfg := &YdbStorageParams{
		Database: database,
		Instance: instance,
		Tables:   []string{testTableName},
		Token:    model.SecretString(token),
	}

	loadWithLimits := func(maxLen int, maxSize int) [][]abstract.ChangeItem {
		cfg.MaxBatchLen = maxLen
		cfg.MaxBatchSize = maxSize

		storage, err := NewStorage(cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)

		res := make([][]abstract.ChangeItem, 0)
		require.NoError(t, storage.LoadTable(context.Background(), abstract.TableDescription{Name: testTableName}, func(input []abstract.ChangeItem) error {
			res = append(res, input)
			return nil
		}))

		return res
	}

	t.Run("ZeroMaxBatchLenAndSize", func(t *testing.T) {
		loadResult := loadWithLimits(0, 0)

		require.Len(t, loadResult, 1)
		require.Len(t, loadResult[0], 2)
	})

	t.Run("LowMaxBatchLen", func(t *testing.T) {
		loadResult := loadWithLimits(1, 1024*1024)

		require.Len(t, loadResult, 2)
		require.Len(t, loadResult[0], 1)
		require.Len(t, loadResult[1], 1)
	})

	t.Run("LowMaxBatchSize", func(t *testing.T) {
		loadResult := loadWithLimits(10000, 32)

		require.Len(t, loadResult, 2)
		require.Len(t, loadResult[0], 1)
		require.Len(t, loadResult[1], 1)
	})

	t.Run("HighMaxBatchLenAndSize", func(t *testing.T) {
		loadResult := loadWithLimits(10000, 1024*1024)

		require.Len(t, loadResult, 1)
		require.Len(t, loadResult[0], 2)
	})
}

func prepareTestTable(t *testing.T, tableName string) {
	driver := ydbrecipe.Driver(t)

	err := driver.Table().Do(context.Background(), func(ctx context.Context, tableSession table.Session) error {
		return tableSession.CreateTable(ctx, tableName,
			options.WithColumn("id", types.Optional(types.TypeUint64)),
			options.WithColumn("value", types.Optional(types.TypeString)),
			options.WithPrimaryKeyColumn("id"),
		)
	})
	require.NoError(t, err)

	ydbtable.ExecQueries(t, driver, []string{
		fmt.Sprintf("--!syntax_v1\nUPSERT INTO `%s` (id, value) VALUES  (%d, '%s');", path.Base(tableName), 1, "32bytes-aaaaaaaaaaaaaaaaaaaaaaaa"),
		fmt.Sprintf("--!syntax_v1\nUPSERT INTO `%s` (id, value) VALUES  (%d, '%s');", path.Base(tableName), 2, "32bytes-aaaaaaaaaaaaaaaaaaaaaaaa"),
	})
}
