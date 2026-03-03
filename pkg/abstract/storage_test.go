package abstract

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewTableIDFromStringPg(t *testing.T) {
	t.Run("schemaName unquoted, tableName unquoted", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`cms.FooContents`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "cms", Name: "FooContents"}, *test)
	})

	t.Run("schemaName unquoted, tableName quoted", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`cms."FooContents"`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "cms", Name: "FooContents"}, *test)
	})

	t.Run("schemaName quoted, tableName unquoted", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`"cms".FooContents`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "cms", Name: "FooContents"}, *test)
	})

	t.Run("schemaName quoted, tableName quoted", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`"cms"."FooContents"`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "cms", Name: "FooContents"}, *test)
	})

	t.Run("schemaName quoted with dots, tableName quoted with dots", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`"schema.name.with.dots.in.it"."table.name.with.dots.in.it"`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "schema.name.with.dots.in.it", Name: "table.name.with.dots.in.it"}, *test)
	})

	t.Run("schemaName quoted with dots, tableName quoted", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`"cm.s"."FooContents"`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "cm.s", Name: "FooContents"}, *test)
	})

	t.Run("schemaName quoted with dots, tableName unquoted", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`"cm.s".FooContents`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "cm.s", Name: "FooContents"}, *test)
	})

	t.Run("schemaName quoted, tableName quoted with dots", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`"cms"."Foo.Contents"`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "cms", Name: "Foo.Contents"}, *test)
	})

	t.Run("only unquoted tableName", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`FooContents`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "", Name: "FooContents"}, *test)
	})

	t.Run("only unquoted tableName with public replacement", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`FooContents`, true)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "public", Name: "FooContents"}, *test)
	})

	t.Run("only quoted tableName", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`"FooContents"`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "", Name: "FooContents"}, *test)
	})

	t.Run("only quoted tableName with public replacement", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`"FooContents"`, true)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "public", Name: "FooContents"}, *test)
	})

	t.Run("only quoted tableName with dots", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`"F.o.o"`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "", Name: "F.o.o"}, *test)
	})

	t.Run("schema unquoted, star", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`public.*`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "public", Name: "*"}, *test)
	})

	t.Run("star", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`*`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "", Name: "*"}, *test)
	})

	t.Run("star with public replacement", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`*`, true)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "public", Name: "*"}, *test)
	})

	t.Run("schemaName unquoted, tableName unquoted, special characters", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`cms!FooContents`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: "", Name: "cms!FooContents"}, *test)
	})

	t.Run("only double quotes in table and schema", func(t *testing.T) {
		test, err := NewTableIDFromStringPg(`"""""".""""`, false)
		require.NoError(t, err)
		require.Equal(t, TableID{Namespace: `""`, Name: `"`}, *test)
	})

	t.Run("empty quoted schema and table", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`"".""`, false)
		require.Error(t, err)
	})

	t.Run("empty quoted table", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`""`, false)
		require.Error(t, err)
	})

	t.Run("empty quoted table with missing double-escape", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`"""`, false)
		require.Error(t, err)
	})

	t.Run("empty quoted schema and table with missing double-escape", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`""."""`, false)
		require.Error(t, err)
	})

	t.Run("empty quoted schema and table with missing double-escape", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`"schema".""table"`, false)
		require.Error(t, err)
	})

	t.Run("forgotten closing quote", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`"cms"."FooContents`, false)
		require.Error(t, err)
	})

	t.Run("schemaName quoted, tableName has forgotten closing quote", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`"cms"."FooContents`, false)
		require.Error(t, err)
	})

	t.Run("schemaName unquoted, tableName has forgotten closing quote", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`cms."FooContents`, false)
		require.Error(t, err)
	})

	t.Run("schemaName has forgotten closing quote", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`"cms.FooContents`, false)
		require.Error(t, err)
	})

	t.Run("empty schemaName, tableName unquoted", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`.blablabla`, false)
		require.Error(t, err)
	})

	t.Run("empty schemaName, tableName quoted", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`."blablabla"`, false)
		require.Error(t, err)
	})

	t.Run("unquoted schemaName, empty tableName unquoted", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`blablabla.`, false)
		require.Error(t, err)
	})

	t.Run("quoted schemaName, empty tableName unquoted", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`"blablabla".`, false)
		require.Error(t, err)
	})

	t.Run("unquoted schemaName, empty tableName quoted", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`blablabla.""`, false)
		require.Error(t, err)
	})

	t.Run("quoted schemaName, empty tableName quoted", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`"blablabla".""`, false)
		require.Error(t, err)
	})

	t.Run("unescaped double quotes", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`"cms"!"FooContents"`, false)
		require.Error(t, err)
	})

	t.Run("part of name double quoted", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`"cms"!FooContents`, false)
		require.Error(t, err)
	})

	t.Run("second part of name double quoted", func(t *testing.T) {
		_, err := NewTableIDFromStringPg(`cms!"FooContents"`, false)
		require.Error(t, err)
	})
}

func TestTableIDFQTN(t *testing.T) {
	t.Run("Two normal parts", func(t *testing.T) {
		result := NewTableID("schema", "table").Fqtn()
		require.Equal(t, `"schema"."table"`, result)
	})

	t.Run("Without schema", func(t *testing.T) {
		result := NewTableID("", "table").Fqtn()
		require.Equal(t, `"table"`, result)
	})

	t.Run("Star without schema", func(t *testing.T) {
		result := NewTableID("", "*").Fqtn()
		require.Equal(t, `*`, result)
	})

	t.Run("Star with schema", func(t *testing.T) {
		result := NewTableID("schema", "*").Fqtn()
		require.Equal(t, `"schema".*`, result)
	})

	t.Run("Schema with double quote", func(t *testing.T) {
		result := NewTableID("schema\"", "table").Fqtn()
		require.Equal(t, `"schema"""."table"`, result)
	})

	t.Run("Schema with two double quotes", func(t *testing.T) {
		result := NewTableID("schema\"\"", "table").Fqtn()
		require.Equal(t, `"schema"""""."table"`, result)
	})

	t.Run("Name with double quote", func(t *testing.T) {
		result := NewTableID("schema", "tab\"le").Fqtn()
		require.Equal(t, `"schema"."tab""le"`, result)
	})

	t.Run("Schema public and table with two double quotes", func(t *testing.T) {
		result := NewTableID("public", "tab\"\"le").Fqtn()
		require.Equal(t, `"public"."tab""""le"`, result)
	})
}

func TestTableIDsIntersection(t *testing.T) {
	t.Run("tii_one_and_one", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("schema", "table"),
			},
			[]TableID{
				*NewTableID("schema", "table"),
			},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("schema", "table"),
			},
			result,
		)
	})

	t.Run("tii_one_and_empty", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("schema", "table"),
			},
			[]TableID{},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("schema", "table"),
			},
			result,
		)
	})

	t.Run("tii_empty_and_one", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{},
			[]TableID{
				*NewTableID("schema", "table"),
			},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("schema", "table"),
			},
			result,
		)
	})

	t.Run("tii_two_and_one", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("schema", "table"),
				*NewTableID("schema", "table2"),
			},
			[]TableID{
				*NewTableID("schema", "table"),
			},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("schema", "table"),
			},
			result,
		)
	})

	t.Run("tii_one_and_two", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("schema", "table"),
			},
			[]TableID{
				*NewTableID("schema", "table1"),
				*NewTableID("schema", "table"),
			},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("schema", "table"),
			},
			result,
		)
	})

	t.Run("tii_one_and_public", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("schema", "table"),
			},
			[]TableID{
				*NewTableID("public", "table"),
			},
		)
		require.Equal(
			t,
			[]TableID{},
			result,
		)
	})

	t.Run("tii_public_and_one", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("public", "table"),
			},
			[]TableID{
				*NewTableID("schema", "table"),
			},
		)
		require.Equal(
			t,
			[]TableID{},
			result,
		)
	})

	t.Run("tii_noschema_and_one", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("", "table"),
			},
			[]TableID{
				*NewTableID("schema", "table"),
			},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("schema", "table"),
			},
			result,
		)
	})

	t.Run("tii_one_and_noschema", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("schema", "table"),
			},
			[]TableID{
				*NewTableID("", "table"),
			},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("schema", "table"),
			},
			result,
		)
	})

	t.Run("tii_two_and_schema_star", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("schema", "table"),
				*NewTableID("public", "table"),
			},
			[]TableID{
				*NewTableID("schema", "*"),
			},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("schema", "table"),
			},
			result,
		)
	})

	t.Run("tii_schema_star_and_two", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("schema", "*"),
			},
			[]TableID{
				*NewTableID("schema", "table"),
				*NewTableID("public", "table"),
			},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("schema", "table"),
			},
			result,
		)
	})

	t.Run("tii_schema_star_and_one_schema_star", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("schema", "*"),
			},
			[]TableID{
				*NewTableID("schema", "*"),
				*NewTableID("public", "table"),
			},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("schema", "*"),
			},
			result,
		)
	})

	t.Run("tii_star_and_one", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("", "*"),
			},
			[]TableID{
				*NewTableID("public", "table"),
			},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("public", "table"),
			},
			result,
		)
	})

	t.Run("tii_three_and_star", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("public", "table"),
				*NewTableID("public", "table2"),
				*NewTableID("schema", "table3"),
			},
			[]TableID{
				*NewTableID("", "*"),
			},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("public", "table"),
				*NewTableID("public", "table2"),
				*NewTableID("schema", "table3"),
			},
			result,
		)
	})

	t.Run("tii_star_and_none", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("", "*"),
			},
			[]TableID{},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("", "*"),
			},
			result,
		)
	})

	t.Run("tii_star_and_star", func(t *testing.T) {
		result := TableIDsIntersection(
			[]TableID{
				*NewTableID("", "*"),
			},
			[]TableID{
				*NewTableID("", "*"),
			},
		)
		require.Equal(
			t,
			[]TableID{
				*NewTableID("", "*"),
			},
			result,
		)
	})
}
