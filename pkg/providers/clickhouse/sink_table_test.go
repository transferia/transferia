package clickhouse

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/topology"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/yt/go/schema"
)

var defCols = abstract.NewTableSchema([]abstract.ColSchema{
	{
		ColumnName: "_timestamp",
		DataType:   schema.TypeDatetime.String(),
	},
	{
		ColumnName: "id",
		DataType:   schema.TypeInt64.String(),
		PrimaryKey: true,
		Required:   true,
	},
	{
		ColumnName: "payload",
		DataType:   schema.TypeAny.String(),
	},
})

func emptyRegistry() metrics.Registry {
	return solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()})
}

func makeSchema(cols *abstract.TableSchema, isUpdateable bool) (*Schema, *sinkTable) {
	config := &model.ChDestination{
		Database:                "db",
		MdbClusterID:            "asd",
		IsUpdateable:            isUpdateable,
		UpsertAbsentToastedRows: false,
	}
	config.WithDefaults()

	table := &sinkTable{
		server:    nil,
		tableName: "test_table",
		config:    config.ToReplicationFromPGSinkParams().MakeChildServerParams("1.1.1.1"),
		logger:    logger.Log,
		cols:      cols,
		cluster:   &sinkCluster{topology: topology.NewTopology("asd", true)},
		timezone:  time.UTC,
	}

	return NewSchema(cols.Columns(), table.config.SystemColumnsFirst(), table.tableName), table
}

func TestGenerateDDLUpdatable(t *testing.T) {
	sch, table := makeSchema(defCols, false)
	ddl := table.generateDDL(sch.abstractCols(), true)
	require.Equal(
		t,
		"CREATE TABLE IF NOT EXISTS `test_table` ON CLUSTER `asd` (`_timestamp` Nullable(DateTime), `id` Int64, `payload` Nullable(String)) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{shard}/db.test_table_cdc', '{replica}') ORDER BY (`id`)",
		ddl,
	)
}

func TestGenerateDDLNotUpdatable(t *testing.T) {
	sch, table := makeSchema(defCols, true)
	ddl := table.generateDDL(sch.abstractCols(), true)
	require.Equal(
		t,
		"CREATE TABLE IF NOT EXISTS `test_table` ON CLUSTER `asd` (`_timestamp` Nullable(DateTime), `id` Int64, `payload` Nullable(String), `__data_transfer_commit_time` UInt64, `__data_transfer_delete_time` UInt64) ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/db.test_table_cdc', '{replica}', __data_transfer_commit_time) ORDER BY (`id`)",
		ddl,
	)
}

func TestGenerateDDLWithNullableKey(t *testing.T) {
	columns := abstract.NewTableSchema([]abstract.ColSchema{
		{
			ColumnName: "id",
			DataType:   schema.TypeInt64.String(),
			PrimaryKey: true,
			Required:   false,
		},
		{
			ColumnName: "payload",
			DataType:   schema.TypeAny.String(),
		},
	})
	sch, table := makeSchema(columns, false)
	require.Equal(
		t,
		"CREATE TABLE IF NOT EXISTS `test_table` (`id` Nullable(Int64), `payload` Nullable(String)) ENGINE=MergeTree() ORDER BY (`id`) SETTINGS allow_nullable_key = 1",
		table.generateDDL(sch.abstractCols(), false),
	)
}

func TestTable_doOperation_updatable_delete(t *testing.T) {
	cols := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", PrimaryKey: true, Required: true},
		{ColumnName: "old_status", DataType: "utf8"},
		{ColumnName: "new_status", DataType: "utf8"},
		{ColumnName: "claim_id", DataType: "int64"},
		{ColumnName: "event_time", DataType: "utf8"},
		{ColumnName: "comment", DataType: "utf8"},
		{ColumnName: "ticket", DataType: "utf8"},
		{ColumnName: "old_current_point", DataType: "int64"},
		{ColumnName: "new_current_point", DataType: "int64"},
	})

	_, table := makeSchema(cols, true)

	table.metrics = stats.NewChStats(emptyRegistry())

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO `db`.`test_table` \\(`id`,`old_status`,`new_status`,`claim_id`,`event_time`,`comment`,`ticket`,`old_current_point`,`new_current_point`,`__data_transfer_commit_time`,`__data_transfer_delete_time`\\) VALUES \\(\\?,\\?,\\?,\\?,\\?,\\?,\\?,\\?,\\?,\\?,\\?\\)")
	mock.ExpectExec("INSERT INTO `db`.`test_table` \\(`id`,`old_status`,`new_status`,`claim_id`,`event_time`,`comment`,`ticket`,`old_current_point`,`new_current_point`,`__data_transfer_commit_time`,`__data_transfer_delete_time`\\) VALUES \\(\\?,\\?,\\?,\\?,\\?,\\?,\\?,\\?,\\?,\\?,\\?\\)").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	changeItemJSON := `
	{
		"id":1179091267,
		"nextlsn":1124107798696,
		"commitTime":1617637873269348000,
		"txPosition":0,
		"kind":"delete",
		"schema":"cargo_claims",
		"table":"claim_audit",
		"columnnames":null,
		"table_schema":[
			{"path":"","name":"id",               "type":"int64","key":true, "required":false},
			{"path":"","name":"old_status",       "type":"utf8", "key":false,"required":false},
			{"path":"","name":"new_status",       "type":"utf8", "key":false,"required":false},
			{"path":"","name":"claim_id",         "type":"int64","key":false,"required":false},
			{"path":"","name":"event_time",       "type":"utf8", "key":false,"required":false},
			{"path":"","name":"comment",          "type":"utf8", "key":false,"required":false},
			{"path":"","name":"ticket",           "type":"utf8", "key":false,"required":false},
			{"path":"","name":"old_current_point","type":"int64","key":false,"required":false},
			{"path":"","name":"new_current_point","type":"int64","key":false,"required":false}
		],
		"oldkeys":{
			"keynames":["id"],
			"keytypes":["bigint"],
			"keyvalues":[1608155]
		}
	}
	`

	var changeItem abstract.ChangeItem
	err = json.Unmarshal([]byte(changeItemJSON), &changeItem)
	require.NoError(t, err)

	err = doOperation(table, tx, []abstract.ChangeItem{changeItem})
	require.NoError(t, err)
}

var toastedVal = "zKlReE2GlXsGjqw5Fidw74PcozFUOC4QaQMDwecRNqRBW3MaEkKyFDDGnB0sXG38bWpKc7yd1bE4ASUYKn44NG1jcNOs2DTIKOesODDjmIkySAyAH0LmKe2lZLecFG3ojitq8yOhJY9gQoEoD2CAJDcjFRMrZOQiFuiE0sIYEKl0Mh6E0IUfu7veWSaFaUIWt65aqXyh4aLDIVP0cwZu5i2e4AA0Do7mqNTuo5wg7lmwKEWO2u2fXrKX1wa7Qqmre7EFwvoQ6ZjkeT5sRjPLzeaqAJbb6HhVowtW87bKT7jYViMONnNV2hnCmVKU4nFnNrQGrR2rUXvXu82Glu2cuPJlyb6dRwevJJmwHsOiw0bNxSkpJOrFQhdH0R5d9nzFavI2s2U11WP55dJjcGrXbkxl5XNCGVsaP4CQoAS6IefrQSVL1TUl2iDhX1h9IbH6HUH5CTNjLuPj0BddZx09OeJ8JJ7e1TFysbzxxRnGA9ANzXEyNfH6YkBv7ioydgAoTj81CZZWTah9Lbhi2KScKw5QdweOuncgTU95204WIHFynPNofoCAMDb4uLYyAw5mu54QN9ryD6rDRGKueZhdPmaKNb7CNwqNuWS3033hvPgDOjNcPebQuU5oDkZ8QtzDIOtlddUtvsIRriU027o8rqwe0T2gjGSmyVKRTDCNyWonSHkcvzWBHk7m6U7H5WWlPLwiutqlf4eNIjIukYR46AyLCt08cHsAnjENKsfSsFObCwjtWti1HyodX8NYUoyC1A9LIqI42VCGtskg9BtAGnoiMqkzH6ZzgeTy771W5hOJPobUegGSdU3LvxxWifaKIfgLy8wHw8gPvFkGJc1vqeJQqJZS0kMdDieGYtJtvpxA7YuiotRC9KNHuWm6E3VaaeXIycHIsR80W1hPcMAHofaJVjekvvc1PoEpeC1HD3bty9rQNZGA9aSJ2AVdv1MdMXlcZi7k2qBcxJmkdLDvd6cEO6zWprc7fmD3OhcHU5qNvRmvddUJ09KH0HmeIFdfZwtSNjrnuPGjYZb14ziMVrZjz6B8VHRw7a9UT1IlR7fAmMT4869Kw24JemvCbiiby2V7uTAo15sAuM6EN5RCA20yFqJ5d1WhlYXoUhC9GJ8x688EXWndUnSVBYSeBorJpAn5QA2CU6sfCgbgaVuLCJYNxu6MMS8WJGKidoCXmbrdkQMB8FdUR6S8HXNGT2izb150P2doc0YwVx0XQJqjjbXK1IMpTjDtF5clKsWmKJ6x3LfGc5rBEcaitAH1gdMBzHqwZpNjaAAPCsOo2sJ8JHXdwtpqJpC1qbMZPCbWQP6Xq5HWIs61IeUD9yZwpmeqWuFYTnaclyc0aiojx5FqgMq3SLxiYdK9tqgrTM6z4Ng8ttD7UnMNKLmNoVS8OgTfiC0jiH9XIfZL1uczWmI2ssMnx9CPCXpJRG0uo5jbgmXp80FHJW0DEBT6QBHhWl98NvsJaNQBZM1v3vOyotJ1L7Dsf3STmd2REK3H8hzWRzgEnxroYtNQPIuvTe0VIAKZy23M76UZDEVufwCCLfN0n6Ljb4wat6dHIdvIvm5ND2Pqf1bY4oDle3qgYqo8czc8CwmC9ONlmtiit3bHjHopVqu8GOnO82bQ1O4QA4wgGEBwGK1cU7Us6Q4XWRIrvJt1hEZ8o1A32diqgWSbMTUjo44eei7xrkMhRsaXewyRAZBbeAEFAtayhINgLdplqxvPyC7hh4tfwxE06po5AidFS8UG9L8jy0LkztvCBZQZs1AwYtorVYp8EIDoC2NpNtQJeI4iwUE060G2xwusW4OlCxx2dPI2WKfSrWOS2GL8wiD8TJeVbaXaSx9npddNI13A1aInd39ZFOFI2Nd3WjJsSMyGXG5roCFOqprqTPGnSCVSBDehveEjAgMIoR38CuFvZFU1UlKdC0wCBzMK3H8sgtrkYwYxBHUzzqhBfnTVckHcDujNJrufwpPiuJfxY7SelvL9spsKv7Ub5f8h04ZGKM3AdYkhKGrewvxNae3DOuuKr5f3Zu04aGiLXGUV2rzW4"

func TestTable_doOperation_failed_on_TOAST(t *testing.T) {
	cols := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", PrimaryKey: true},
		{ColumnName: "val", DataType: "utf8"},
		{ColumnName: "val2", DataType: "int64"},
	})

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	_, table := makeSchema(cols, true)
	sinkServer := NewSinkServerImpl(table.config, db, semver.Version{Major: 20}, time.UTC, table.logger, table.metrics, nil)

	table.metrics = stats.NewChStats(emptyRegistry())
	table.server = sinkServer

	mock.ExpectBegin()
	rows := sqlmock.NewRows([]string{"id", "val"}).
		AddRow(5, toastedVal)
	mock.ExpectQuery("SELECT id,val FROM `db`\\.`test_table` WHERE \\(id=\\?\\) ORDER BY __data_transfer_commit_time DESC LIMIT 1 BY \\(id\\)").WithArgs(5).WillReturnRows(rows)

	mock.ExpectPrepare("INSERT INTO `db`.`test_table` \\(`id`,`val`,`val2`,`__data_transfer_commit_time`,`__data_transfer_delete_time`\\) VALUES \\(\\?,\\?,\\?,\\?,\\?\\)")
	mock.ExpectExec("INSERT INTO `db`.`test_table` \\(`id`,`val`,`val2`,`__data_transfer_commit_time`,`__data_transfer_delete_time`\\) VALUES \\(\\?,\\?,\\?,\\?,\\?\\)").WithArgs(5, toastedVal, 6, sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO `db`.`test_table` \\(`id`,`val`,`val2`,`__data_transfer_commit_time`,`__data_transfer_delete_time`\\) VALUES \\(\\?,\\?,\\?,\\?,\\?\\)").WithArgs(10, nil, nil, sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	changeItemJSONs := []string{
		`
	{
		"id": 781499,
		"nextlsn": 20283749192,
		"commitTime": 1628109253511396000,
		"txPosition": 0,
		"kind": "update",
		"schema": "public",
		"table": "timmyb32r_test_tm_2174_pg_src2",
		"columnnames": ["id", "val2"],
		"columnvalues": [5, 6],
		"table_schema": [
			{"path": "", "name": "id",   "type": "int32", "key": true,  "required": false},
			{"path": "", "name": "val",  "type": "utf8",  "key": false, "required": false},
			{"path": "", "name": "val2", "type": "int32", "key": false, "required": false}
        ],
		"oldkeys": {
			"keynames": ["id"],
			"keytypes": ["integer"],
			"keyvalues": [5]
		},
		"tx_id": "",
		"query": ""
	}`, `
	{
		"id":1179091267,
		"nextlsn":1124107798696,
		"commitTime":1617637873269348000,
		"txPosition":0,
		"kind":"delete",
		"schema": "public",
		"table": "timmyb32r_test_tm_2174_pg_src2",
		"columnnames":null,
		"table_schema": [
			{"path": "", "name": "id",   "type": "int32", "key": true,  "required": false},
			{"path": "", "name": "val",  "type": "utf8",  "key": false, "required": false},
			{"path": "", "name": "val2", "type": "int32", "key": false, "required": false}
        ],
		"oldkeys": {
			"keynames": ["id"],
			"keytypes": ["integer"],
			"keyvalues": [10]
		}
	}`,
	}

	changeItems := make([]abstract.ChangeItem, 2)
	err = json.Unmarshal([]byte(changeItemJSONs[0]), &changeItems[0])
	require.NoError(t, err)
	err = json.Unmarshal([]byte(changeItemJSONs[1]), &changeItems[1])
	require.NoError(t, err)

	err = doOperation(table, tx, changeItems)
	require.NoError(t, err)
}

func TestTable_doOperation_ok_on_TOAST_alias_and_materialized(t *testing.T) {
	cols := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", PrimaryKey: true},
		{ColumnName: "Qval", DataType: "utf8", Expression: "ALIAS:blablabla"},
		{ColumnName: "Wval", DataType: "utf8", Expression: "MATERIALIZED:blablabla"},
		{ColumnName: "val2", DataType: "int64"},
	})

	_, table := makeSchema(cols, true)

	table.metrics = stats.NewChStats(emptyRegistry())

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO `db`.`test_table` \\(`id`,`val2`,`__data_transfer_commit_time`,`__data_transfer_delete_time`\\) VALUES \\(\\?,\\?,\\?,\\?\\)")
	mock.ExpectExec("INSERT INTO `db`.`test_table` \\(`id`,`val2`,`__data_transfer_commit_time`,`__data_transfer_delete_time`\\) VALUES \\(\\?,\\?,\\?,\\?\\)").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	changeItemJSON := `
	{
		"id": 781499,
		"nextlsn": 20283749192,
		"commitTime": 1628109253511396000,
		"txPosition": 0,
		"kind": "update",
		"schema": "public",
		"table": "timmyb32r_test_tm_2174_pg_src2",
		"columnnames": ["id", "val2"],
		"columnvalues": [5, 6],
		"table_schema": [
			{"path": "", "name": "id",   "type": "int32", "key": true,  "required": false},
			{"path": "", "name": "Qval",  "type": "utf8",  "key": false, "required": false},
			{"path": "", "name": "Wval",  "type": "utf8",  "key": false, "required": false},
			{"path": "", "name": "val2", "type": "int32", "key": false, "required": false}
        ],
		"oldkeys": {
			"keynames": ["id"],
			"keytypes": ["integer"],
			"keyvalues": [5]
		},
		"tx_id": "",
		"query": ""
	}
	`

	var changeItem abstract.ChangeItem
	err = json.Unmarshal([]byte(changeItemJSON), &changeItem)
	require.NoError(t, err)

	err = doOperation(table, tx, []abstract.ChangeItem{changeItem})
	require.NoError(t, err)
}

func TestTable_normalizeColumnNamesOrder(t *testing.T) {
	masterJSON := `
	{
		"kind": "update",
		"schema": "public",
		"table": "timmyb32r_test_tm_2174_pg_src2",
		"columnnames": ["id", "val1", "val2"],
		"columnvalues": [4, 5, 6],
		"table_schema": [
			{"path": "", "name": "id",   "type": "int32", "key": true,  "required": false},
			{"path": "", "name": "val1", "type": "int32", "key": false, "required": false},
			{"path": "", "name": "val2", "type": "int32", "key": false, "required": false}
		]
	}
	`
	var masterChangeItem abstract.ChangeItem
	require.NoError(t, json.Unmarshal([]byte(masterJSON), &masterChangeItem))
	t.Run("noop", func(t *testing.T) {
		var changeItem abstract.ChangeItem
		var err error
		secondJSON := `
	{
		"kind": "update",
		"schema": "public",
		"table": "timmyb32r_test_tm_2174_pg_src2",
		"columnnames": ["id", "val1", "val2"],
		"columnvalues": [7, 8, 9],
		"table_schema": [
			{"path": "", "name": "id",   "type": "int32", "key": true,  "required": false},
			{"path": "", "name": "val1", "type": "int32", "key": false, "required": false},
			{"path": "", "name": "val2", "type": "int32", "key": false, "required": false}
		]
	}
	`
		require.NoError(t, json.Unmarshal([]byte(secondJSON), &changeItem))
		items, err := normalizeColumnNamesOrder([]abstract.ChangeItem{masterChangeItem, changeItem})
		require.NoError(t, err)
		require.Len(t, items, 2)
		require.Equal(t, items[0].ColumnNames, items[1].ColumnNames)
	})
	t.Run("simple reorder", func(t *testing.T) {
		var changeItem abstract.ChangeItem
		var err error
		secondJSON := `
	{
		"kind": "update",
		"schema": "public",
		"table": "timmyb32r_test_tm_2174_pg_src2",
		"columnnames": ["id", "val2", "val1"],
		"columnvalues": [7, 9, 8],
		"table_schema": [
			{"path": "", "name": "id",   "type": "int32", "key": true,  "required": false},
			{"path": "", "name": "val1", "type": "int32", "key": false, "required": false},
			{"path": "", "name": "val2", "type": "int32", "key": false, "required": false}
		]
	}
	`
		require.NoError(t, json.Unmarshal([]byte(secondJSON), &changeItem))
		items, err := normalizeColumnNamesOrder([]abstract.ChangeItem{masterChangeItem, changeItem})
		require.NoError(t, err)
		require.Len(t, items, 2)
		require.Equal(t, items[0].ColumnNames, items[1].ColumnNames)
		require.Equal(t, []interface{}{json.Number("7"), json.Number("8"), json.Number("9")}, items[1].ColumnValues)
	})
	t.Run("missed column", func(t *testing.T) {
		var changeItem abstract.ChangeItem
		secondJSON := `
	{
		"kind": "update",
		"schema": "public",
		"table": "timmyb32r_test_tm_2174_pg_src2",
		"columnnames": ["id", "val1"],
		"columnvalues": [7, 8],
		"table_schema": [
			{"path": "", "name": "id",   "type": "int32", "key": true,  "required": false},
			{"path": "", "name": "val1", "type": "int32", "key": false, "required": false},
			{"path": "", "name": "val2", "type": "int32", "key": false, "required": false}
		]
	}
	`
		require.NoError(t, json.Unmarshal([]byte(secondJSON), &changeItem))
		_, err := normalizeColumnNamesOrder([]abstract.ChangeItem{masterChangeItem, changeItem})
		require.Error(t, err)
	})
	t.Run("columns mismatch", func(t *testing.T) {
		var changeItem abstract.ChangeItem
		secondJSON := `
	{
		"kind": "update",
		"schema": "public",
		"table": "timmyb32r_test_tm_2174_pg_src2",
		"columnnames": ["id", "val1", "val3"],
		"columnvalues": [7, 8, 9],
		"table_schema": [
			{"path": "", "name": "id",   "type": "int32", "key": true,  "required": false},
			{"path": "", "name": "val1", "type": "int32", "key": false, "required": false},
			{"path": "", "name": "val2", "type": "int32", "key": false, "required": false}
		]
	}
	`
		require.NoError(t, json.Unmarshal([]byte(secondJSON), &changeItem))
		_, err := normalizeColumnNamesOrder([]abstract.ChangeItem{masterChangeItem, changeItem})
		require.Error(t, err)
	})
}

func TestSplitRowsBySchema(t *testing.T) {
	t.Run("same schema", func(t *testing.T) {
		rows := []abstract.ChangeItem{
			{
				TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
					{ColumnName: "id"},
					{ColumnName: "val"},
				}),
				ColumnNames: []string{
					"id", "val",
				},
			},
			{
				TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
					{ColumnName: "id"},
					{ColumnName: "val"},
				}),
				ColumnNames: []string{
					"id", "val",
				},
			},
		}

		batches := splitRowsBySchema(rows)
		require.Len(t, batches, 1)
		require.EqualValues(t, rows, batches[0])
	})

	t.Run("different schemas", func(t *testing.T) {
		rows := []abstract.ChangeItem{
			{
				TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
					{ColumnName: "id"},
					{ColumnName: "val"},
				}),
				ColumnNames: []string{
					"id", "val",
				},
			},
			{
				TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
					{ColumnName: "id"},
				}),
				ColumnNames: []string{
					"id",
				},
			},
			{
				TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
					{ColumnName: "id"},
					{ColumnName: "val2"},
				}),
				ColumnNames: []string{
					"id", "va2",
				},
			},
		}

		batches := splitRowsBySchema(rows)
		require.Len(t, batches, 3)
		require.EqualValues(t, rows[0:1], batches[0])
		require.EqualValues(t, rows[1:2], batches[1])
		require.EqualValues(t, rows[2:3], batches[2])
	})

	t.Run("insert with outdated schema", func(t *testing.T) {
		rows := []abstract.ChangeItem{
			{
				TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
					{ColumnName: "id"},
					{ColumnName: "val"},
				}),
				Kind: abstract.InsertKind,
				ColumnNames: []string{
					"id", "val",
				},
			},
			{ // outdated schema
				TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
					{ColumnName: "id"},
					{ColumnName: "val"},
				}),
				Kind: abstract.InsertKind,
				ColumnNames: []string{
					"id",
				},
			},
		}

		batches := splitRowsBySchema(rows)
		require.Len(t, batches, 2)
		require.EqualValues(t, rows[0:1], batches[0])

		require.Len(t, batches[1], 1)
		require.EqualValues(t, []string{"id"}, batches[1][0].ColumnNames)
		require.EqualValues(t, []abstract.ColSchema{{ColumnName: "id"}}, batches[1][0].TableSchema.Columns())
	})
}

func TestCompareColumnSets(t *testing.T) {
	schema1 := []abstract.ColSchema{
		{ColumnName: "id"},
		{ColumnName: "val1"},
	}
	schema2 := []abstract.ColSchema{
		{ColumnName: "id"},
		{ColumnName: "val2"},
	}

	added, removed := compareColumnSets(schema1, schema2)
	require.EqualValues(t, []abstract.ColSchema{{ColumnName: "val2"}}, added)
	require.EqualValues(t, []abstract.ColSchema{{ColumnName: "val1"}}, removed)

	added, removed = compareColumnSets(schema1, schema1)
	require.Empty(t, added)
	require.Empty(t, removed)
}
