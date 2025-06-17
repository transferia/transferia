//go:build !disable_ydb_provider

package ydb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/util"
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
	endpoint, ok := os.LookupEnv("YDB_ENDPOINT")
	if !ok {
		t.Fail()
	}
	prefix, ok := os.LookupEnv("YDB_DATABASE")
	if !ok {
		t.Fail()
	}
	token, ok := os.LookupEnv("YDB_TOKEN")
	if !ok {
		token = "anyNotEmptyString"
	}

	src := &YdbSource{
		Token:    model.SecretString(token),
		Database: prefix,
		Instance: endpoint,
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
		Database: prefix,
		Token:    model.SecretString(token),
		Instance: endpoint,
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
	endpoint, ok := os.LookupEnv("YDB_ENDPOINT")
	if !ok {
		t.Fail()
	}

	prefix, ok := os.LookupEnv("YDB_DATABASE")
	if !ok {
		t.Fail()
	}

	token, ok := os.LookupEnv("YDB_TOKEN")
	if !ok {
		token = "anyNotEmptyString"
	}

	src := YdbSource{
		Token:              model.SecretString(token),
		Database:           prefix,
		Instance:           endpoint,
		Tables:             nil,
		TableColumnsFilter: nil,
		SubNetworkID:       "",
		Underlay:           false,
		ServiceAccountID:   "",
	}

	st, err := NewStorage(src.ToStorageParams(), solomon.NewRegistry(solomon.NewRegistryOpts()))

	require.NoError(t, err)

	cfg := YdbDestination{
		Database: prefix,
		Token:    model.SecretString(token),
		Instance: endpoint,
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
	for table := range tables {
		if len(table.Name) > 10 && table.Name[:10] == "table_list" {
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
