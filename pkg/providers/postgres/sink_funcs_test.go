package postgres

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestBuildMultiRowInsertStatements_BasicWithOnConflict(t *testing.T) {
	table := `"public"."t"`
	schema := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "val", DataType: "utf8", OriginalType: "pg:text"},
	}
	keys := map[string][]string{
		table: {`"id"`},
	}
	items := []abstract.ChangeItem{
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val"}, ColumnValues: []any{int64(1), "a"}},
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val"}, ColumnValues: []any{int64(2), "b"}},
	}

	stmts, err := BuildBulkInsertQuery(table, schema, keys, 1024, items)
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
	schema := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "meta", DataType: "any", OriginalType: "pg:jsonb"},
		{ColumnName: "tags", DataType: "any", OriginalType: "pg:integer[]"},
		{ColumnName: "raw", DataType: "bytes", OriginalType: "pg:bytea"},
	}
	keys := map[string][]string{
		table: {`"id"`},
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

	stmts, err := BuildBulkInsertQuery(table, schema, keys, 1024, items)
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
	keys := map[string][]string{
		table: {`"id"`},
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

	stmts, err := BuildBulkInsertQuery(table, schema, keys, 1024, items)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.NotContains(t, stmts[0].query, `"gen"`)
	require.NotContains(t, stmts[0].query, "ignored")
	require.Contains(t, stmts[0].query, `("id", "val") values`)
}

func TestBuildMultiRowInsertStatements_SplitsByMaxBytes(t *testing.T) {
	table := "t"
	schema := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64"},
		{ColumnName: "val", DataType: "utf8"},
	}
	keys := map[string][]string{}

	long := strings.Repeat("x", 70)
	items := []abstract.ChangeItem{
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val"}, ColumnValues: []any{int64(1), long}},
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val"}, ColumnValues: []any{int64(2), long}},
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val"}, ColumnValues: []any{int64(3), long}},
	}

	stmts, err := BuildBulkInsertQuery(table, schema, keys, 120, items)
	require.NoError(t, err)
	require.Len(t, stmts, 3)
	for _, stmt := range stmts {
		require.Equal(t, 1, stmt.rows)
		require.True(t, strings.HasSuffix(stmt.query, ";"))
	}
}

func TestBuildMultiRowInsertStatements_ErrOnIncompatibleColumnLayout(t *testing.T) {
	table := `"public"."t"`
	keys := map[string][]string{
		table: {`"id"`},
	}
	schema := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
		{ColumnName: "val", DataType: "utf8", OriginalType: "pg:text"},
	}
	items := []abstract.ChangeItem{
		{Kind: abstract.InsertKind, ColumnNames: []string{"id", "val"}, ColumnValues: []any{int64(1), "a"}},
		{Kind: abstract.InsertKind, ColumnNames: []string{"val", "id"}, ColumnValues: []any{"b", int64(2)}},
	}

	stmts, err := BuildBulkInsertQuery(table, schema, keys, 1024, items)
	require.Error(t, err)
	require.Nil(t, stmts)
	require.Contains(t, err.Error(), "incompatible change item column layout")
}

func TestBuildBulkInsertQuery_Errors(t *testing.T) {
	table := `"public"."t"`
	schema0 := []abstract.ColSchema{
		{ColumnName: "id", DataType: "int64", OriginalType: "pg:bigint"},
	}
	keys := map[string][]string{
		table: {`"id"`},
	}

	t.Run("empty items returns nil nil", func(t *testing.T) {
		stmts, err := BuildBulkInsertQuery(table, schema0, keys, 1024, nil)
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

		stmts, err := BuildBulkInsertQuery(table, schema, nil, 1024, items)
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

		stmts, err := BuildBulkInsertQuery(table, schema, nil, 1024, items)
		require.Error(t, err)
		require.Nil(t, stmts)
		require.Contains(t, err.Error(), `multi-row insert requires column "val"`)
	})

	t.Run("single tuple too large returns error", func(t *testing.T) {
		localTable := "t"
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

		stmts, err := BuildBulkInsertQuery(localTable, schema, nil, 120, items)
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
	keys := map[string][]string{
		table: {keyColumn},
	}
	stmts, err := BuildBulkInsertQuery(table, schema, keys, 1024, rows)
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
		currStmts, err := BuildBulkInsertQuery(table, schema, keys, 1024, rows)
		if err != nil {
			b.Fatalf("buildBulkInsertQuery failed: %v", err)
		}
		if len(currStmts) == 0 {
			b.Fatal("buildBulkInsertQuery returned no statements")
		}
	}
}
