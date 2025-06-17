//go:build !disable_ydb_provider

package ydb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.ytsaurus.tech/yt/go/schema"
)

func TestSinker_Push(t *testing.T) {
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

	cfg := YdbDestination{
		Database:           prefix,
		Token:              model.SecretString(token),
		Instance:           endpoint,
		DropUnknownColumns: true,
		ShardCount:         -1,
	}
	cfg.WithDefaults()
	sinker, err := NewSinker(logger.Log, &cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	t.Run("inserts", func(t *testing.T) {
		data := make([]abstract.ChangeItem, len(rows))
		for i, r := range rows {
			names := make([]string, 0)
			vals := make([]interface{}, 0)
			for _, v := range demoSchema.Columns() {
				names = append(names, v.ColumnName)
				vals = append(vals, r[v.ColumnName])
			}
			data[i] = abstract.ChangeItem{
				Kind:         abstract.InsertKind,
				Schema:       "foo",
				Table:        "inserts_test",
				ColumnNames:  names,
				ColumnValues: vals,
				TableSchema:  demoSchema,
			}
		}
		require.NoError(t, sinker.Push(data))
	})
	testSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: string(schema.TypeInt32), PrimaryKey: true},
		{ColumnName: "val", DataType: string(schema.TypeString)},
	})
	testSchemaMultiKey := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id1", DataType: string(schema.TypeInt32), PrimaryKey: true},
		{ColumnName: "id2", DataType: string(schema.TypeInt32), PrimaryKey: true},
		{ColumnName: "val", DataType: string(schema.TypeString)},
	})
	t.Run("many upserts", func(t *testing.T) {
		data := make([]abstract.ChangeItem, batchSize*2)
		for i := range batchSize * 2 {
			kind := abstract.InsertKind
			if i > 0 {
				kind = abstract.UpdateKind
			}
			data[i] = abstract.ChangeItem{
				Kind:         kind,
				Schema:       "foo",
				Table:        "many_upserts_test",
				ColumnNames:  []string{"id", "val"},
				ColumnValues: []interface{}{1, fmt.Sprint(i)},
				TableSchema:  testSchema,
			}
		}
		require.NoError(t, sinker.Push(data))

		db, err := ydb.Open(
			context.Background(),
			sugar.DSN(endpoint, prefix),
			ydb.WithAccessTokenCredentials(token),
		)
		require.NoError(t, err)

		expectedVal := fmt.Sprint(batchSize*2 - 1)
		selectQuery(t, db, `
		--!syntax_v1
		SELECT val FROM foo_many_upserts_test WHERE id = 1;
		`, types.NullableUTF8Value(&expectedVal))
	})
	t.Run("inserts+delete", func(t *testing.T) {
		require.NoError(t, sinker.Push([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			Schema:       "foo",
			Table:        "inserts_delete_test",
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []interface{}{1, "test"},
			TableSchema:  testSchema,
		}}))
		require.NoError(t, sinker.Push([]abstract.ChangeItem{{
			Kind:        abstract.DeleteKind,
			Schema:      "foo",
			Table:       "inserts_delete_test",
			TableSchema: testSchema,
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"id"},
				KeyTypes:  nil,
				KeyValues: []interface{}{1},
			},
		}}))
	})
	t.Run("inserts+delete with compound key", func(t *testing.T) {
		require.NoError(t, sinker.Push([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			Schema:       "foo",
			Table:        "inserts_delete_test_with_compound_key",
			ColumnNames:  []string{"id1", "id2", "val"},
			ColumnValues: []interface{}{1, 0, "test"},
			TableSchema:  testSchemaMultiKey,
		}}))
		require.NoError(t, sinker.Push([]abstract.ChangeItem{{
			Kind:        abstract.DeleteKind,
			Schema:      "foo",
			Table:       "inserts_delete_test_with_compound_key",
			TableSchema: testSchemaMultiKey,
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"id1", "id2"},
				KeyTypes:  nil,
				KeyValues: []interface{}{1, 0},
			},
		}}))
	})
	t.Run("inserts_altering_table", func(t *testing.T) {
		require.NoError(t, sinker.Push([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			Schema:       "foo",
			Table:        "inserts_altering_table",
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []interface{}{1, "test"},
			TableSchema:  testSchema,
		}}))
		originalColumns := testSchema.Columns()
		addColumn := abstract.ColSchema{
			ColumnName: "add",
			DataType:   string(schema.TypeString),
		}
		addToDelColumn := abstract.ColSchema{
			ColumnName: "will_be_deleted",
			DataType:   string(schema.TypeString),
		}
		// YDB sinker caches state of tables and won't recognize need to change it
		// so we need "run new transfer" to be able alter table
		sinkerAdd, err := NewSinker(logger.Log, &cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)
		require.NoError(t, sinkerAdd.Push([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			Schema:       "foo",
			Table:        "inserts_altering_table",
			ColumnNames:  []string{"id", "val", "add", "will_be_deleted"},
			ColumnValues: []interface{}{2, "test", "any", "any2"},
			TableSchema:  abstract.NewTableSchema(append(originalColumns, addColumn, addToDelColumn)),
		}}))
		sinkerDel, err := NewSinker(logger.Log, &cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
		require.NoError(t, err)
		delColumnsSchema := abstract.NewTableSchema(append(originalColumns, addColumn))
		require.NoError(t, sinkerDel.Push([]abstract.ChangeItem{{
			Kind:         abstract.InsertKind,
			Schema:       "foo",
			Table:        "inserts_altering_table",
			ColumnNames:  []string{"id", "val", "add"},
			ColumnValues: []interface{}{3, "test", "any"},
			TableSchema:  delColumnsSchema,
		}}))
		for i := 1; i <= 3; i++ {
			require.NoError(t, sinkerDel.Push([]abstract.ChangeItem{{
				Kind:        abstract.DeleteKind,
				Schema:      "foo",
				Table:       "inserts_altering_table",
				TableSchema: delColumnsSchema,
				OldKeys: abstract.OldKeysType{
					KeyNames:  []string{"id"},
					KeyTypes:  nil,
					KeyValues: []interface{}{i},
				},
			}}))
		}
	})
	t.Run("drop", func(t *testing.T) {
		data := make([]abstract.ChangeItem, len(rows))
		for i, r := range rows {
			names := make([]string, 0)
			vals := make([]interface{}, 0)
			for _, v := range demoSchema.Columns() {
				names = append(names, v.ColumnName)
				vals = append(vals, r[v.ColumnName])
			}
			data[i] = abstract.ChangeItem{
				Kind:         abstract.InsertKind,
				Schema:       "foo",
				Table:        "drop_test",
				ColumnNames:  names,
				ColumnValues: vals,
				TableSchema:  demoSchema,
			}
		}
		require.NoError(t, sinker.Push(data))
		require.NoError(t, sinker.Push([]abstract.ChangeItem{
			{
				Kind:   abstract.DropTableKind,
				Schema: "foo",
				Table:  "drop_test",
			},
		}))
	})

	tableSchemaWithFlowColumn := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "id", DataType: string(schema.TypeInt32), PrimaryKey: true},
		// flow is a hidden internal data type, but user should suffer from it. No docs of it whatsoever, but if no escaping happens, it blows up
		{ColumnName: "flow", DataType: string(schema.TypeString), PrimaryKey: true},
		// list, as well as flow, has the same bad behaviour
		{ColumnName: "list", DataType: string(schema.TypeString), PrimaryKey: true},
	})
	t.Run("inserts_with_odd_colname", func(t *testing.T) {
		require.NoError(t, sinker.Push([]abstract.ChangeItem{
			{
				Kind:         abstract.InsertKind,
				Schema:       "foo",
				Table:        "inserts_with_odd_colname",
				ColumnNames:  []string{"id", "flow", "list"},
				ColumnValues: []interface{}{1, "flowjob", "listing is 300 bucks"},
				TableSchema:  tableSchemaWithFlowColumn,
			},
		}))
		require.NoError(t, sinker.Push([]abstract.ChangeItem{
			{
				Kind:   abstract.DeleteKind,
				Schema: "foo",
				Table:  "inserts_with_odd_colname",
				OldKeys: abstract.OldKeysType{
					KeyNames:  []string{"id", "flow", "list"},
					KeyTypes:  nil,
					KeyValues: []interface{}{1, "flowjob", "listing is 300 bucks"},
				},
				TableSchema: tableSchemaWithFlowColumn,
			},
		}))
	})
	require.NoError(t, sinker.Push([]abstract.ChangeItem{
		{
			Kind:   abstract.DropTableKind,
			Schema: "foo",
			Table:  "inserts_delete_test",
		},
		{
			Kind:   abstract.DropTableKind,
			Schema: "foo",
			Table:  "inserts_delete_test_with_compound_key",
		},
		{
			Kind:   abstract.DropTableKind,
			Schema: "foo",
			Table:  "inserts_altering_table",
		},
		{
			Kind:   abstract.DropTableKind,
			Schema: "foo",
			Table:  "inserts_test",
		},
		{
			Kind:   abstract.DropTableKind,
			Schema: "foo",
			Table:  "many_upserts_test",
		},
		{
			Kind:   abstract.DropTableKind,
			Schema: "foo",
			Table:  "inserts_with_odd_colname",
		},
	}))
}

func TestSinker_insertQuery(t *testing.T) {
	s := &sinker{config: &YdbDestination{}}
	q := s.insertQuery(
		"test_table",
		[]abstract.ColSchema{
			{ColumnName: "_timestamp", DataType: "DateTime"},
			{ColumnName: "_partition", DataType: string(schema.TypeString)},
			{ColumnName: "_offset", DataType: string(schema.TypeInt64)},
			{ColumnName: "_idx", DataType: string(schema.TypeInt32)},
			{ColumnName: "_rest", DataType: string(schema.TypeAny)},
			{ColumnName: "raw_value", DataType: string(schema.TypeString)},
		},
	)

	require.Equal(t, `--!syntax_v1
DECLARE $batch AS List<
	Struct<
		`+"`_timestamp`"+`:Datetime?,
		`+"`_partition`"+`:Utf8?,
		`+"`_offset`"+`:Int64?,
		`+"`_idx`"+`:Int32?,
		`+"`_rest`"+`:Json?,
		`+"`raw_value`"+`:Utf8?
	>
>;
UPSERT INTO `+"`test_table`"+` (
		`+"`_timestamp`"+`,
		`+"`_partition`"+`,
		`+"`_offset`"+`,
		`+"`_idx`"+`,
		`+"`_rest`"+`,
		`+"`raw_value`"+`
)
SELECT
	`+"`_timestamp`"+`,
	`+"`_partition`"+`,
	`+"`_offset`"+`,
	`+"`_idx`"+`,
	`+"`_rest`"+`,
	`+"`raw_value`"+`
FROM AS_TABLE($batch)
`, q)
}

func TestSinker_deleteQuery(t *testing.T) {
	s := &sinker{config: &YdbDestination{}}
	q := s.deleteQuery(
		"flow_table",
		[]abstract.ColSchema{
			{ColumnName: "_timestamp", DataType: "DateTime"},
			{ColumnName: "_partition", DataType: string(schema.TypeString)},
			{ColumnName: "_offset", DataType: string(schema.TypeInt64)},
			{ColumnName: "_idx", DataType: string(schema.TypeInt32)},
			{ColumnName: "_rest", DataType: string(schema.TypeAny)},
			// flow is a hidden internal data type, but user should suffer from it. No docs of it whatsoever, but if no escaping happens, it blows up
			{ColumnName: "flow", DataType: string(schema.TypeString)},
			{ColumnName: "list", DataType: string(schema.TypeString)},
		},
	)

	require.Equal(t, `--!syntax_v1
DECLARE $batch AS Struct<
	`+"`_timestamp`"+`:Datetime?,
	`+"`_partition`"+`:Utf8?,
	`+"`_offset`"+`:Int64?,
	`+"`_idx`"+`:Int32?,
	`+"`_rest`"+`:Json?,
	`+"`flow`"+`:Utf8?,
	`+"`list`"+`:Utf8?
>;
DELETE FROM `+"`flow_table`"+`
WHERE 1=1

	and `+"`_timestamp`"+` = $batch.`+"`_timestamp`"+`
	and `+"`_partition`"+` = $batch.`+"`_partition`"+`
	and `+"`_offset`"+` = $batch.`+"`_offset`"+`
	and `+"`_idx`"+` = $batch.`+"`_idx`"+`
	and `+"`_rest`"+` = $batch.`+"`_rest`"+`
	and `+"`flow`"+` = $batch.`+"`flow`"+`
	and `+"`list`"+` = $batch.`+"`list`"+`
`, q)
}

func TestIsPrimaryKey(t *testing.T) {
	type testCase struct {
		objKey                string
		ydbType               types.Type
		column                abstract.ColSchema
		isTableColumnOriented bool
		expectingError        bool
		result                bool
	}
	tests := []testCase{
		{
			objKey:                "TypeCanBePk_ColumnIsNotPk_RowTable",
			ydbType:               types.TypeUint8,
			column:                abstract.ColSchema{PrimaryKey: false},
			isTableColumnOriented: false,
			expectingError:        false,
			result:                false,
		},
		{
			objKey:                "TypeCanBePk_ColumnIsNotPk_ColTable",
			ydbType:               types.TypeUint8,
			column:                abstract.ColSchema{PrimaryKey: false},
			isTableColumnOriented: true,
			expectingError:        false,
			result:                false,
		},
		{
			objKey:                "TypeCanBePk_ColumnIsPk_RowTable",
			ydbType:               types.TypeUint8,
			column:                abstract.ColSchema{PrimaryKey: true},
			isTableColumnOriented: false,
			expectingError:        false,
			result:                true,
		},
		{
			objKey:                "TypeCanBePk_ColumnIsPk_ColTable",
			ydbType:               types.TypeUint8,
			column:                abstract.ColSchema{PrimaryKey: true},
			isTableColumnOriented: true,
			expectingError:        false,
			result:                true,
		},
		{
			objKey:                "TypePkOnlyForRow_ColumnIsPk_RowTable",
			ydbType:               types.TypeTzDate,
			column:                abstract.ColSchema{PrimaryKey: true},
			isTableColumnOriented: false,
			expectingError:        false,
			result:                true,
		},
		{
			objKey:                "TypePkOnlyForRow_ColumnIsPk_ColTable",
			ydbType:               types.TypeTzDate,
			column:                abstract.ColSchema{PrimaryKey: true},
			isTableColumnOriented: true,
			expectingError:        true,
			result:                false,
		},
		{
			objKey:                "TypePkOnlyForColumn_ColumnIsPk_RowTable",
			ydbType:               TypeYdbDecimal,
			column:                abstract.ColSchema{PrimaryKey: true},
			isTableColumnOriented: false,
			expectingError:        true,
			result:                false,
		},
		{
			objKey:                "TypePkOnlyForColumn_ColumnIsPk_ColTable",
			ydbType:               TypeYdbDecimal,
			column:                abstract.ColSchema{PrimaryKey: true},
			isTableColumnOriented: true,
			expectingError:        false,
			result:                true,
		},
		{
			objKey:                "TypeCanNotBePK_ColumnIsPk_RowTable",
			ydbType:               types.TypeJSON,
			column:                abstract.ColSchema{PrimaryKey: true},
			isTableColumnOriented: false,
			expectingError:        true,
			result:                false,
		},
		{
			objKey:                "TypeCanNotBePK_ColumnIsPk_ColTable",
			ydbType:               types.TypeJSON,
			column:                abstract.ColSchema{PrimaryKey: true},
			isTableColumnOriented: true,
			expectingError:        true,
			result:                false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.objKey, func(t *testing.T) {
			config := YdbDestination{IsTableColumnOriented: tc.isTableColumnOriented}
			s := sinker{config: &config}
			isPk, err := s.isPrimaryKey(tc.ydbType, tc.column)
			require.Equal(t, tc.result, isPk)
			require.Equal(t, tc.expectingError, err != nil)
		})
	}
}

func TestCreateTableQuery(t *testing.T) {
	columns := []ColumnTemplate{
		{Name: "col1", Type: "int", NotNull: false},
		{Name: "col2", Type: "bool", NotNull: false},
	}
	table := CreateTableTemplate{
		Path:                  "table_path",
		Columns:               columns,
		Keys:                  []string{"col1"},
		ShardCount:            1,
		IsTableColumnOriented: false,
		DefaultCompression:    "lz4",
	}

	var query strings.Builder
	require.NoError(t, createTableQueryTemplate.Execute(&query, table))

	expected := "--!syntax_v1\n" +
		"CREATE TABLE `table_path` (\n\t" +
		"`col1` int , \n\t" +
		"`col2` bool , \n\t\t" +
		"PRIMARY KEY (`col1`),\n\t" +
		"FAMILY default (\n\t" +
		"\tCOMPRESSION = \"lz4\"\n\t" +
		")" +
		"\n)" +
		"\n" +
		"\nWITH (\n\t" +
		"\tUNIFORM_PARTITIONS = 1\n);\n"

	require.Equal(t, expected, query.String())
}

func selectQuery(t *testing.T, ydbConn *ydb.Driver, query string, expected types.Value) {
	var val types.Value
	err := ydbConn.Table().Do(context.Background(), func(ctx context.Context, session table.Session) (err error) {
		writeTx := table.TxControl(
			table.BeginTx(
				table.WithSerializableReadWrite(),
			),
			table.CommitTx(),
		)

		_, res, err := session.Execute(ctx, writeTx, query, nil)
		require.NoError(t, err)

		for res.NextResultSet(ctx) {
			for res.NextRow() {
				err = res.Scan(&val)
				require.NoError(t, err)
			}
		}

		require.Equal(t, expected, val)
		return err
	})
	require.NoError(t, err)
}
