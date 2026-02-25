package postgres

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

type arrUint8 []uint8

func (a arrUint8) Value() (driver.Value, error) {
	return []uint8{'{', '}'}, nil
}

type arrUint8WithQuote []uint8

func (a arrUint8WithQuote) Value() (driver.Value, error) {
	return []uint8{'{', '"', 'q', '"', ':', '"', '\'', '"', '}'}, nil
}

func TestPartialUpdate(t *testing.T) {
	schema := []abstract.ColSchema{
		{ColumnName: "worker_id", DataType: "any", OriginalType: "pg:character varying(32)", PrimaryKey: true},
		{ColumnName: "company_id", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "total_income", DataType: "float64", OriginalType: "pg:numeric(15,2)"},
		{ColumnName: "blocked_income", DataType: "float64", OriginalType: "pg:numeric(15,2)"},
		{ColumnName: "toloka_fee", DataType: "float64", OriginalType: "pg:numeric(15,4)"},
		{ColumnName: "blocked_toloka_fee", DataType: "float64", OriginalType: "pg:numeric(15,4)"},
		{ColumnName: "submitted_assignments", DataType: "int64", OriginalType: "pg:bigint"},
	}
	var changes []abstract.ChangeItem
	require.NoError(t, json.Unmarshal([]byte(`
[{"kind":"update","schema":"public","table":"worker_to_company","columnnames":["worker_id","company_id","total_income","blocked_income","toloka_fee","blocked_toloka_fee","submitted_assignments"],"columntypes":["character varying(32)","bigint","numeric(15,2)","numeric(15,2)","numeric(15,4)","numeric(15,4)","bigint"],"columnvalues":["0df01541642df57712f218a0a7891422",388,0.01,0.03,0.0050,0.0160,5],"oldkeys":{"keynames":["worker_id","company_id","total_income","blocked_income","toloka_fee","blocked_toloka_fee","submitted_assignments"],"keytypes":["character varying(32)","bigint","numeric(15,2)","numeric(15,2)","numeric(15,4)","numeric(15,4)","bigint"],"keyvalues":["0df01541642df57712f218a0a7891422",388,0.01,0.03,0.0050,0.0150,5]}}]
`), &changes))

	t.Run("per tx push should include only changed values", func(t *testing.T) {
		sink := new(sink)
		sink.config = (&PgDestination{PerTransactionPush: true}).ToSinkParams()
		query, err := sink.buildQuery("worker_to_company", schema, changes)
		require.NoError(t, err)
		require.Equal(t, query, `update worker_to_company set "blocked_toloka_fee" = '0.0160'::numeric(15,4) where "worker_id" = '0df01541642df57712f218a0a7891422'::character varying(32) and "company_id" = '388'::bigint and "total_income" = '0.01'::numeric(15,2) and "blocked_income" = '0.03'::numeric(15,2) and "toloka_fee" = '0.0050'::numeric(15,4) and "blocked_toloka_fee" = '0.0150'::numeric(15,4) and "submitted_assignments" = '5'::bigint;`)
	})
}

func TestPgJSONInsertSerialization(t *testing.T) {
	var err error

	var schema []abstract.ColSchema
	err = json.Unmarshal([]byte(`[{"path":"","name":"flags","type":"any","key":false,"required":false}]`), &schema)
	require.NoError(t, err)
	schema[0].OriginalType = "pg:jsonb"

	sink := new(sink)
	sink.config = (&PgDestination{}).ToSinkParams()

	var rows []abstract.ChangeItem
	err = json.Unmarshal([]byte(`[{"id":0,"nextlsn":17171568749512,"commitTime":1636482041578468000,"txPosition":0,"kind":"insert","schema":"public","table":"services_service","columnnames":["flags"],"columnvalues":[{}],"table_schema":[{"path":"","name":"flags","type":"any","key":false,"required":false}],"oldkeys":{},"tx_id":"","query":""}]`), &rows)
	require.NoError(t, err)
	rows[0].ColumnValues[0] = arrUint8{}
	query, err := sink.buildQuery("blablabla", schema, rows)
	require.NoError(t, err)
	require.Equal(t, `insert into blablabla ("flags") values ('{}'::jsonb);`, query)

	var rows2 []abstract.ChangeItem
	err = json.Unmarshal([]byte(`[{"id":0,"nextlsn":17171568749512,"commitTime":1636482041578468000,"txPosition":0,"kind":"insert","schema":"public","table":"services_service","columnnames":["flags"],"columnvalues":[{}],"table_schema":[{"path":"","name":"flags","type":"any","key":false,"required":false}],"oldkeys":{},"tx_id":"","query":""}]`), &rows2)
	require.NoError(t, err)
	rows2[0].ColumnValues[0] = arrUint8WithQuote{}
	query2, err := sink.buildQuery("blablabla", schema, rows2)
	require.NoError(t, err)
	require.Equal(t, `insert into blablabla ("flags") values ('{"q":"''"}'::jsonb);`, query2)
}

func TestRepresent(t *testing.T) {
	type testCase struct {
		inSchema string
		inValue  interface{}
		outValue string
	}

	testCases := []testCase{
		{ // json (got from yt-source yson) - output should be array in JSON notation
			`{"path":"","name":"t_yson","type":"any","key":false,"required":false,"original_type":"pg:jsonb"}`,
			[]interface{}{uint64(100), uint64(200), uint64(300)},
			`'[100,200,300]'`,
		},
		{ // array (got from pg-source int[]) - output should be in postgres-array notation
			`{"path":"","name":"arr_i","type":"any","key":false,"required":false,"original_type":"pg:integer[]"}`,
			[]interface{}{"1", "2"},
			`'{1,2}'`,
		},
		{ // array of strings
			`{"path":"","name":"arr_str","type":"utf8","key":false,"required":false,"original_type":"pg:character varying[]"}`,
			[]interface{}{"varchar_example", "varchar_example"},
			`'{varchar_example,varchar_example}'`,
		},
		{ // array of strings
			`{"path":"","name":"arr_character_varying_","type":"any","key":false,"required":false,"original_type":"pg:character varying(5)[]"}`,
			[]interface{}{"varc", "varc"},
			`'{varc,varc}'`,
		},
		{ // array of strings, any type but no orgiginal
			`{"path":"","name":"yt_arr","type":"any","key":false,"required":false}`,
			[]interface{}{"yandex_staff_history", "maps_yandex_staff_actual"},
			`'["yandex_staff_history","maps_yandex_staff_actual"]'`,
		},
		{ // YSON struct, any type but no orgiginal
			`{"path":"","name":"yt_arr","type":"any","key":false,"required":false}`,
			map[string]interface{}{"foo": 123, "bar": map[string]string{"baz": "booz"}},
			`'{"bar":{"baz":"booz"},"foo":123}'`,
		},
	}

	for i, currTestCase := range testCases {
		t.Run(fmt.Sprintf("tc/%v", i), func(t *testing.T) {
			var schema abstract.ColSchema
			err := json.Unmarshal([]byte(currTestCase.inSchema), &schema)
			require.NoError(t, err)
			newVal, err := Represent(currTestCase.inValue, schema)
			require.NoError(t, err)
			require.Equal(t, currTestCase.outValue, newVal)
		})
	}
}

func TestBuildMultiRowInsertStatements_BasicWithOnConflict(t *testing.T) {
	table := `"public"."t"`
	s := &sink{
		keys: map[string][]string{
			table: {`"id"`},
		},
	}
	schema := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "val", DataType: "utf8", OriginalType: "pg:text"},
	}
	items := []abstract.ChangeItem{
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val"}, ColumnValues: []any{int64(1), "a"}},
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val"}, ColumnValues: []any{int64(2), "b"}},
	}

	stmts, err := s.buildBulkInsertQuery(table, schema, items)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	require.Equal(t,
		`insert into "public"."t" ("id", "val") values (`+
			`'1'::bigint, 'a'::text), (`+
			`'2'::bigint, 'b'::text)`+
			` on conflict ("id") do update set ("id", "val")=row(excluded."id", excluded."val");`,
		stmts[0].query,
	)
	require.Equal(t, 2, stmts[0].rows)
}

func TestBuildMultiRowInsertStatements_SkipsGeneratedColumns(t *testing.T) {
	table := `"public"."t"`
	s := &sink{
		keys: map[string][]string{
			table: {`"id"`},
		},
	}
	schema := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "val", DataType: "utf8", OriginalType: "pg:text"},
		{ColumnName: "gen", DataType: "utf8", OriginalType: "pg:text", Expression: "now()"},
	}
	items := []abstract.ChangeItem{
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val", "gen"}, ColumnValues: []any{int64(1), "a", "ignored"}},
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val", "gen"}, ColumnValues: []any{int64(2), "b", "ignored"}},
	}

	stmts, err := s.buildBulkInsertQuery(table, schema, items)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.NotContains(t, stmts[0].query, `"gen"`)
	require.NotContains(t, stmts[0].query, "ignored")
	require.Contains(t, stmts[0].query, `("id", "val") values`)
}

func TestBuildMultiRowInsertStatements_SplitsByMaxBytes(t *testing.T) {
	old := maxPostgresQueryBytes
	maxPostgresQueryBytes = 120
	defer func() { maxPostgresQueryBytes = old }()

	table := "t"
	s := &sink{keys: map[string][]string{}}
	schema := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64"},
		{ColumnName: "val", DataType: "utf8"},
	}

	long := strings.Repeat("x", 70)
	items := []abstract.ChangeItem{
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val"}, ColumnValues: []any{int64(1), long}},
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val"}, ColumnValues: []any{int64(2), long}},
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val"}, ColumnValues: []any{int64(3), long}},
	}

	stmts, err := s.buildBulkInsertQuery(table, schema, items)
	require.NoError(t, err)
	require.Len(t, stmts, 3)
	for _, stmt := range stmts {
		require.Equal(t, 1, stmt.rows)
		require.True(t, strings.HasSuffix(stmt.query, ";"))
	}
}

func TestBuildInsertQuery_WithOnConflictAndCasts(t *testing.T) {
	table := `"public"."t"`
	s := &sink{
		config: (&PgDestination{}).ToSinkParams(),
		keys: map[string][]string{
			table: {`"id"`},
		},
	}
	schema := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "val", DataType: "utf8", OriginalType: "pg:text"},
	}
	row := abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		ColumnNames:  []string{"id", "val"},
		ColumnValues: []any{int64(10), "hello"},
	}

	rev := abstract.MakeMapColNameToIndex(schema)
	q, err := s.buildInsertQuery(table, schema, row, rev)
	require.NoError(t, err)
	require.Equal(t, `insert into "public"."t" ("id", "val") values ('10'::bigint, 'hello'::text) on conflict ("id") do update set ("id", "val")=row(excluded."id", excluded."val");`, q)
}

func TestBuildInsertQuery_WithoutKeys_NoOnConflict(t *testing.T) {
	table := `"public"."t"`
	s := &sink{
		config: (&PgDestination{}).ToSinkParams(),
		keys:   map[string][]string{},
	}
	schema := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "val", DataType: "utf8", OriginalType: "pg:text"},
	}
	row := abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		ColumnNames:  []string{"id", "val"},
		ColumnValues: []any{int64(1), "a"},
	}

	rev := abstract.MakeMapColNameToIndex(schema)
	q, err := s.buildInsertQuery(table, schema, row, rev)
	require.NoError(t, err)
	require.Equal(t, `insert into "public"."t" ("id", "val") values ('1'::bigint, 'a'::text);`, q)
}

func TestBuildInsertQuery_SkipsGeneratedColumns(t *testing.T) {
	table := `"public"."t"`
	s := &sink{
		config: (&PgDestination{}).ToSinkParams(),
		keys: map[string][]string{
			table: {`"id"`},
		},
	}
	schema := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "val", DataType: "utf8", OriginalType: "pg:text"},
		{ColumnName: "gen", DataType: "utf8", OriginalType: "pg:text", Expression: "now()"},
	}
	row := abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		ColumnNames:  []string{"id", "val", "gen"},
		ColumnValues: []any{int64(1), "a", "ignored"},
	}

	rev := abstract.MakeMapColNameToIndex(schema)
	q, err := s.buildInsertQuery(table, schema, row, rev)
	require.NoError(t, err)
	require.NotContains(t, q, `"gen"`)
	require.NotContains(t, q, "ignored")
	require.Equal(t, `insert into "public"."t" ("id", "val") values ('1'::bigint, 'a'::text) on conflict ("id") do update set ("id", "val")=row(excluded."id", excluded."val");`, q)
}
