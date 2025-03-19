package token_regexp

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema/ddl_parser/clickhouse_lexer"
	"github.com/transferia/transferia/pkg/util/token_regexp/abstract"
	"github.com/transferia/transferia/pkg/util/token_regexp/op"
)

func TestMatcher(t *testing.T) {
	check := func(t *testing.T, originalStr string, query []any, expectedResult string) {
		tokens := clickhouse_lexer.StringToTokens(originalStr)
		currMatcher := NewTokenRegexp(query)
		results := currMatcher.FindAll(tokens)
		require.Equal(t, expectedResult, results.MatchedSubstring(originalStr))
	}

	t.Run("Match", func(t *testing.T) {
		check(t, "create table qqq on cluster my_cluster", []any{"ON", "CLUSTER"}, "on cluster")
	})

	t.Run("Opt", func(t *testing.T) {
		check(t, "create table qqq on cluster my_cluster", []any{"ON", "CLUSTER", op.Opt("(")}, "on cluster")
	})
	t.Run("Opt", func(t *testing.T) {
		check(t, "create table qqq on cluster", []any{"ON", "CLUSTER", op.Opt("(")}, "on cluster")
	})
	t.Run("Opt, opt triggered, terminal", func(t *testing.T) {
		check(t, "create table qqq on cluster my_cluster", []any{"ON", "CLUSTER", op.Opt("my_cluster")}, "on cluster my_cluster")
	})
	t.Run("Opt, opt triggered, not terminal", func(t *testing.T) {
		check(t, "create table qqq on cluster my_cluster", []any{"create", "table", op.Opt("qqq"), "on", "cluster"}, "create table qqq on cluster")
	})
	t.Run("Opt, opt triggered, not terminal", func(t *testing.T) {
		check(t, "create table qqq on cluster my_cluster", []any{"create", "table", op.Opt("qqqwww"), "on", "cluster"}, "")
	})

	t.Run("MatchNot", func(t *testing.T) {
		check(t, "on cluster ()", []any{"ON", "CLUSTER", op.MatchNot("(")}, "")
	})
	t.Run("MatchNot + opt", func(t *testing.T) {
		check(t, "on cluster ()", []any{"ON", "CLUSTER", op.Opt(op.MatchNot("("))}, "on cluster")
	})

	t.Run("MatchParentheses, matched #0", func(t *testing.T) {
		check(t, "on cluster ()", []any{"ON", "CLUSTER", op.MatchParentheses()}, "on cluster ()")
	})
	t.Run("MatchParentheses, matched #1", func(t *testing.T) {
		check(t, "on cluster ()()", []any{"ON", "CLUSTER", op.MatchParentheses()}, "on cluster ()")
	})
	t.Run("MatchParentheses, not matched #0", func(t *testing.T) {
		check(t, "on cluster (", []any{"ON", "CLUSTER", op.MatchParentheses()}, "")
	})
	t.Run("MatchParentheses, not matched #1", func(t *testing.T) {
		check(t, "on cluster (", []any{"ON", "CLUSTER", op.Opt(op.MatchParentheses())}, "on cluster")
	})

	t.Run("Or", func(t *testing.T) {
		check(t, "on cluster", []any{op.Or("on", "cluster")}, "on")
	})
	t.Run("Or", func(t *testing.T) {
		check(t, "on cluster", []any{op.Or("on")}, "on")
	})
	t.Run("Or", func(t *testing.T) {
		check(t, "on cluster", []any{op.Or("cluster")}, "cluster")
	})

	t.Run("Seq", func(t *testing.T) {
		check(t, "on cluster", []any{op.Seq("on", "cluster")}, "on cluster")
	})

	queryFull := []any{
		"create",
		op.Or("table", op.Seq("materialized", "view")),
		op.Opt(op.Seq("if", "not", "exists")),
		op.Seq(op.Opt(op.Seq(op.AnyToken(), ".")), op.AnyToken()), // tableIdentifier
		op.Opt(op.Seq("uuid", op.AnyToken())),
		op.CapturingGroup(
			op.Opt(op.Seq("on", "cluster", op.Opt(op.AnyToken()))),
		),
		op.MatchParentheses(),
		"engine",
		"=",
		op.CapturingGroup(
			op.AnyToken(),
			op.Opt(op.MatchParentheses()),
		),
	}

	t.Run("Match full", func(t *testing.T) {
		check(t, "create table qqq on cluster my_cluster() engine=q", queryFull, "create table qqq on cluster my_cluster() engine=q")
	})

	t.Run("capturing group", func(t *testing.T) {
		originalStr := "CREATE TABLE qqq on cluster my_cluster() engine=q"
		tokens := clickhouse_lexer.StringToTokens(originalStr)
		currMatcher := NewTokenRegexp(queryFull)
		results := currMatcher.FindAll(tokens)
		require.Equal(t, 1, results.Size())
		matchedPath := results.Index(0)
		capturingGroups := matchedPath.CapturingGroupArr()
		require.Equal(t, 2, len(capturingGroups))
		require.Equal(t, "on cluster my_cluster", abstract.ResolveMatchedOps(originalStr, capturingGroups[0]))
		require.Equal(t, "q", abstract.ResolveMatchedOps(originalStr, capturingGroups[1]))
	})

	t.Run("capturing group with cyrillic", func(t *testing.T) {
		originalStr := "CREATE TABLE db1.cyrillic\n(\n    `id` Int64,\n    `кириллица` String,\n    `value` String\n)\nENGINE = MergeTree\nORDER BY id\nSETTINGS index_granularity = 8192\n"
		tokens := clickhouse_lexer.StringToTokens(originalStr)
		currMatcher := NewTokenRegexp(queryFull)
		results := currMatcher.FindAll(tokens)
		require.Equal(t, 1, results.Size())
		matchedPath := results.Index(0)
		capturingGroups := matchedPath.CapturingGroupArr()
		require.Equal(t, 2, len(capturingGroups))
		require.Equal(t, "", abstract.ResolveMatchedOps(originalStr, capturingGroups[0]))
		require.Equal(t, "MergeTree", abstract.ResolveMatchedOps(originalStr, capturingGroups[1]))
	})
}
