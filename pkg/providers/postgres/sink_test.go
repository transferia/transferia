package postgres

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"go.uber.org/zap"
	corezap "go.ytsaurus.tech/library/go/core/log/zap"
)

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

func TestBuildMultiRowInsertStatements_BasicWithOnConflict(t *testing.T) {
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

func TestBuildMultiRowInsertStatements_WithComplexRepresentations(t *testing.T) {
	table := `"public"."t"`
	s := &sink{
		config: (&PgDestination{}).ToSinkParams(),
		keys: map[string][]string{
			table: {`"id"`},
		},
	}
	schema := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "meta", DataType: "any", OriginalType: "pg:jsonb"},
		{ColumnName: "tags", DataType: "any", OriginalType: "pg:integer[]"},
		{ColumnName: "raw", DataType: "bytes", OriginalType: "pg:bytea"},
	}
	items := []abstract.ChangeItem{
		{
			Kind:        abstract.InsertKind,
			ColumnNames: []string{"id", "meta", "tags", "raw"},
			ColumnValues: []any{
				int64(7),
				map[string]any{"q": "it's"},
				[]interface{}{int64(1), int64(2)},
				[]byte{0x1, 0x2, 0xab},
			},
		},
	}

	stmts, err := s.buildBulkInsertQuery(table, schema, items)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(
		t,
		`insert into "public"."t" ("id", "meta", "tags", "raw") values (`+
			`'7'::bigint, '{"q":"it''s"}'::jsonb, '{1,2}'::integer[], '\x0102ab'::bytea)`+
			` on conflict ("id") do update set ("id", "meta", "tags", "raw")=row(excluded."id", excluded."meta", excluded."tags", excluded."raw");`,
		stmts[0].query,
	)
}

func TestBuildMultiRowInsertStatements_SkipsGeneratedColumns(t *testing.T) {
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
	table := "t"
	s := &sink{
		config: (&PgDestination{MaxPostgresQueryBytes: 120}).ToSinkParams(),
		keys:   map[string][]string{},
	}
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

func TestBuildMultiRowInsertStatements_ErrOnIncompatibleColumnLayout(t *testing.T) {
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
	items := []abstract.ChangeItem{
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val"}, ColumnValues: []any{int64(1), "a"}},
		{Kind: abstract.InsertKind, ColumnNames: []string{"val", "id"}, ColumnValues: []any{"b", int64(2)}},
	}

	stmts, err := s.buildBulkInsertQuery(table, schema, items)
	require.Error(t, err)
	require.Nil(t, stmts)
	require.Contains(t, err.Error(), "incompatible change item column layout")
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

func TestBuildDeleteQuery(t *testing.T) {
	table := `"public"."t"`
	s := &sink{
		logger: &corezap.Logger{L: zap.NewNop()},
	}
	schema := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "tenant", DataType: "utf8", OriginalType: "pg:text"},
	}
	rev := abstract.MakeMapColNameToIndex(schema)

	t.Run("single key", func(t *testing.T) {
		row := abstract.ChangeItem{
			Kind: abstract.DeleteKind,
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"id"},
				KeyValues: []any{int64(42)},
			},
		}

		q, err := s.buildDeleteQuery(table, schema, row, rev)
		require.NoError(t, err)
		require.Equal(t, `DELETE FROM "public"."t" WHERE ("id" = '42'::bigint);`, q)
	})

	t.Run("multiple keys", func(t *testing.T) {
		row := abstract.ChangeItem{
			Kind: abstract.DeleteKind,
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"id", "tenant"},
				KeyValues: []any{int64(7), "acme"},
			},
		}

		q, err := s.buildDeleteQuery(table, schema, row, rev)
		require.NoError(t, err)
		require.Equal(
			t,
			`DELETE FROM "public"."t" WHERE ("id" = '7'::bigint) AND ("tenant" = 'acme'::text);`,
			q,
		)
	})

	t.Run("no old keys", func(t *testing.T) {
		row := abstract.ChangeItem{
			Kind:    abstract.DeleteKind,
			OldKeys: abstract.OldKeysType{},
		}

		q, err := s.buildDeleteQuery(table, schema, row, rev)
		require.Error(t, err)
		require.Empty(t, q)
		require.Contains(t, err.Error(), "Unable to build DELETE query, no key names presented")
	})

	t.Run("key value fallback formatting", func(t *testing.T) {
		row := abstract.ChangeItem{
			Kind: abstract.DeleteKind,
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"id"},
				KeyValues: []any{struct{ X int }{X: 1}},
			},
		}

		q, err := s.buildDeleteQuery(table, schema, row, rev)
		require.NoError(t, err)
		require.Equal(t, `DELETE FROM "public"."t" WHERE ("id" = '{1}'::bigint);`, q)
	})
}

func TestBuildInsertQuery_UpdateBranches(t *testing.T) {
	table := `"public"."t"`
	schema := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint", PrimaryKey: true},
		{ColumnName: "val", DataType: "utf8", OriginalType: "pg:text"},
	}
	rev := abstract.MakeMapColNameToIndex(schema)

	t.Run("update kind with changed keys builds update", func(t *testing.T) {
		s := &sink{
			config: (&PgDestination{}).ToSinkParams(),
			keys: map[string][]string{
				table: {`"id"`},
			},
		}
		row := abstract.ChangeItem{
			Kind:         abstract.UpdateKind,
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []any{int64(2), "updated"},
			TableSchema:  abstract.NewTableSchema(schema),
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"id"},
				KeyValues: []any{int64(1)},
			},
		}

		q, err := s.buildInsertQuery(table, schema, row, rev)
		require.NoError(t, err)
		require.Equal(
			t,
			`update "public"."t" set "id" = '2'::bigint, "val" = 'updated'::text where "id" = '1'::bigint;`,
			q,
		)
	})

	t.Run("update kind without table keys builds update", func(t *testing.T) {
		s := &sink{
			config: (&PgDestination{}).ToSinkParams(),
			keys:   map[string][]string{},
		}
		row := abstract.ChangeItem{
			Kind:         abstract.UpdateKind,
			ColumnNames:  []string{"id", "val"},
			ColumnValues: []any{int64(10), "v2"},
			OldKeys: abstract.OldKeysType{
				KeyNames:  []string{"id"},
				KeyValues: []any{int64(10)},
			},
		}

		q, err := s.buildInsertQuery(table, schema, row, rev)
		require.NoError(t, err)
		require.Equal(
			t,
			`update "public"."t" set "id" = '10'::bigint, "val" = 'v2'::text where "id" = '10'::bigint;`,
			q,
		)
	})
}

func TestBuildBulkInsertQuery_Errors(t *testing.T) {
	table := `"public"."t"`
	s := &sink{
		config: (&PgDestination{}).ToSinkParams(),
		keys: map[string][]string{
			table: {`"id"`},
		},
	}

	t.Run("empty items returns nil nil", func(t *testing.T) {
		stmts, err := s.buildBulkInsertQuery(table, []abstract.ColSchema{
			{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
		}, nil)
		require.NoError(t, err)
		require.Nil(t, stmts)
	})

	t.Run("all columns generated returns error", func(t *testing.T) {
		schema := []abstract.ColSchema{
			{ColumnName: "gen", DataType: "utf8", OriginalType: "pg:text", Expression: "now()"},
		}
		items := []abstract.ChangeItem{
			{
				Kind:         abstract.InsertKind,
				ColumnNames:  []string{"gen"},
				ColumnValues: []any{"ignored"},
			},
		}

		stmts, err := s.buildBulkInsertQuery(table, schema, items)
		require.Error(t, err)
		require.Nil(t, stmts)
		require.Contains(t, err.Error(), "no columns to insert")
	})

	t.Run("missing required column in first row returns error", func(t *testing.T) {
		schema := []abstract.ColSchema{
			{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
			{ColumnName: "val", DataType: "utf8", OriginalType: "pg:text"},
		}
		items := []abstract.ChangeItem{
			{
				Kind:         abstract.InsertKind,
				ColumnNames:  []string{"id"},
				ColumnValues: []any{int64(1)},
			},
		}

		stmts, err := s.buildBulkInsertQuery(table, schema, items)
		require.Error(t, err)
		require.Nil(t, stmts)
		require.Contains(t, err.Error(), `multi-row insert requires column "val"`)
	})

	t.Run("single tuple too large returns error", func(t *testing.T) {
		localTable := "t"
		localSink := &sink{
			config: (&PgDestination{MaxPostgresQueryBytes: 120}).ToSinkParams(),
			keys:   map[string][]string{},
		}
		schema := []abstract.ColSchema{
			{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
			{ColumnName: "val", DataType: "utf8", OriginalType: "pg:text"},
		}
		items := []abstract.ChangeItem{
			{
				Kind:         abstract.InsertKind,
				ColumnNames:  []string{"id", "val"},
				ColumnValues: []any{int64(1), strings.Repeat("x", 300)},
			},
		}

		stmts, err := localSink.buildBulkInsertQuery(localTable, schema, items)
		require.Error(t, err)
		require.Nil(t, stmts)
		require.Contains(t, err.Error(), "single row tuple too large")
	})
}

func BenchmarkPostgresBuildBulkInsertQueryDocumentPropertyAnonymized(b *testing.B) {
	const table = `"policyadmindev"."document_property"`
	schema := benchmarkPostgresDocumentPropertySchema()
	rows := benchmarkPostgresDocumentPropertyRowsAnonymized(1024)
	benchmarkBuildBulkInsertQueryWithConfig(b, table, `"document_property_pk"`, schema, rows)
}

func benchmarkBuildBulkInsertQueryWithConfig(
	b *testing.B,
	table string,
	keyColumn string,
	schema []abstract.ColSchema,
	rows []abstract.ChangeItem,
) {
	s := &sink{
		config: (&PgDestination{}).ToSinkParams(),
		keys: map[string][]string{
			table: {keyColumn},
		},
	}

	stmts, err := s.buildBulkInsertQuery(table, schema, rows)
	if err != nil {
		b.Fatalf("warmup buildBulkInsertQuery failed: %v", err)
	}
	if len(stmts) == 0 {
		b.Fatal("warmup buildBulkInsertQuery returned no statements")
	}
	totalBytes := 0
	for _, stmt := range stmts {
		totalBytes += len(stmt.query)
	}

	b.ReportAllocs()
	b.SetBytes(int64(totalBytes))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stmts, err := s.buildBulkInsertQuery(table, schema, rows)
		if err != nil {
			b.Fatalf("buildBulkInsertQuery failed: %v", err)
		}
		if len(stmts) == 0 {
			b.Fatal("buildBulkInsertQuery returned no statements")
		}
	}
}

func benchmarkPostgresDocumentPropertySchema() []abstract.ColSchema {
	return []abstract.ColSchema{
		{ColumnName: "document_property_pk", DataType: "int64", OriginalType: "pg:bigint", PrimaryKey: true},
		{ColumnName: "document_fk", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "property_type_fk", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "value_str", DataType: "utf8", OriginalType: "pg:text"},
		{ColumnName: "value_num", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "value_ts", OriginalType: "pg:timestamp without time zone"},
		{ColumnName: "value_blob", DataType: "bytes", OriginalType: "pg:bytea"},
	}
}

func benchmarkPostgresDocumentPropertyRowsAnonymized(rowCount int) []abstract.ChangeItem {
	rows := make([]abstract.ChangeItem, rowCount)
	columnNames := []string{
		"document_property_pk",
		"document_fk",
		"property_type_fk",
		"value_str",
		"value_num",
		"value_ts",
		"value_blob",
	}
	textValues := []string{
		"https://example.invalid/resource",
		"asset_0001.bin",
		"scan_0429.jpg",
		"user_a@example.invalid",
		"/path/to/resource/0001.dat",
	}
	propertyTypeValues := []int64{71, 113, 114, 115, 189, 248}
	baseDocFK := int64(67204926)
	basePK := int64(404021704)

	for i := 0; i < rowCount; i++ {
		var valueStr any = textValues[i%len(textValues)]
		var valueNum any = nil
		var valueTS any = nil
		// Keep null-heavy pattern from the source example and mix in number/timestamp rows.
		switch i % 8 {
		case 4:
			valueStr = nil
			valueNum = int64(40 + (i % 10))
		case 6:
			valueTS = time.Date(2024, time.January, 1, 10, 0, 0, 0, time.UTC).Add(time.Minute * time.Duration(i))
		case 7:
			valueStr = nil
			valueNum = int64(100 + (i % 100))
			valueTS = time.Date(2024, time.January, 2, 8, 0, 0, 0, time.UTC).Add(time.Second * time.Duration(i))
		}

		rows[i] = abstract.ChangeItem{
			Kind:        abstract.InsertKind,
			ColumnNames: columnNames,
			ColumnValues: []any{
				basePK + int64(i),
				baseDocFK + int64(i/4),
				propertyTypeValues[i%len(propertyTypeValues)],
				valueStr,
				valueNum,
				valueTS,
				[]byte{},
			},
		}
	}

	return rows
}
