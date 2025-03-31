package token_regexp

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema/ddl_parser/clickhouse_lexer"
	"github.com/transferia/transferia/pkg/util/token_regexp/op"
)

func TestMatcher(t *testing.T) {
	checkMatchedSubstr := func(t *testing.T, originalStr string, query []any, expectedResult string) {
		tokens := clickhouse_lexer.StringToTokens(originalStr)
		currMatcher := NewTokenRegexp(query)
		results := currMatcher.FindAll(tokens)
		require.Equal(t, expectedResult, results.MatchedSubstring(originalStr))
	}

	checkCapturingGroups := func(t *testing.T, originalStr string, query []any, expectedResults []string) {
		tokens := clickhouse_lexer.StringToTokens(originalStr)
		currMatcher := NewTokenRegexp(query)
		results := currMatcher.FindAll(tokens)
		require.Equal(t, 1, results.Size())
		matchedPath := results.Index(0)
		capturingGroups := matchedPath.CapturingGroups()
		require.Equal(t, len(expectedResults), capturingGroups.GroupsNum())
		for expectedResultNum := range expectedResults {
			require.Equal(t, expectedResults[expectedResultNum], capturingGroups.GroupToText(originalStr, expectedResultNum))
		}
	}

	t.Run("Match", func(t *testing.T) {
		checkMatchedSubstr(t, "create table qqq on cluster my_cluster", []any{"ON", "CLUSTER"}, "on cluster")
	})

	t.Run("Opt", func(t *testing.T) {
		checkMatchedSubstr(t, "create table qqq on cluster my_cluster", []any{"ON", "CLUSTER", op.Opt("(")}, "on cluster")
	})
	t.Run("Opt", func(t *testing.T) {
		checkMatchedSubstr(t, "create table qqq on cluster", []any{"ON", "CLUSTER", op.Opt("(")}, "on cluster")
	})
	t.Run("Opt, opt triggered, terminal", func(t *testing.T) {
		checkMatchedSubstr(t, "create table qqq on cluster my_cluster", []any{"ON", "CLUSTER", op.Opt("my_cluster")}, "on cluster my_cluster")
	})
	t.Run("Opt, opt triggered, not terminal", func(t *testing.T) {
		checkMatchedSubstr(t, "create table qqq on cluster my_cluster", []any{"create", "table", op.Opt("qqq"), "on", "cluster"}, "create table qqq on cluster")
	})
	t.Run("Opt, opt triggered, not terminal", func(t *testing.T) {
		checkMatchedSubstr(t, "create table qqq on cluster my_cluster", []any{"create", "table", op.Opt("qqqwww"), "on", "cluster"}, "")
	})

	t.Run("MatchNot", func(t *testing.T) {
		checkMatchedSubstr(t, "on cluster ()", []any{"ON", "CLUSTER", op.MatchNot("(")}, "")
	})
	t.Run("MatchNot + opt", func(t *testing.T) {
		checkMatchedSubstr(t, "on cluster ()", []any{"ON", "CLUSTER", op.Opt(op.MatchNot("("))}, "on cluster")
	})

	t.Run("MatchParentheses, matched #0", func(t *testing.T) {
		checkMatchedSubstr(t, "on cluster ()", []any{"ON", "CLUSTER", op.MatchParentheses()}, "on cluster ()")
	})
	t.Run("MatchParentheses, matched #1", func(t *testing.T) {
		checkMatchedSubstr(t, "on cluster ()()", []any{"ON", "CLUSTER", op.MatchParentheses()}, "on cluster ()")
	})
	t.Run("MatchParentheses, not matched #0", func(t *testing.T) {
		checkMatchedSubstr(t, "on cluster (", []any{"ON", "CLUSTER", op.MatchParentheses()}, "")
	})
	t.Run("MatchParentheses, not matched #1", func(t *testing.T) {
		checkMatchedSubstr(t, "on cluster (", []any{"ON", "CLUSTER", op.Opt(op.MatchParentheses())}, "on cluster")
	})

	t.Run("Or", func(t *testing.T) {
		checkMatchedSubstr(t, "on cluster", []any{op.Or("on", "cluster")}, "on")
	})
	t.Run("Or", func(t *testing.T) {
		checkMatchedSubstr(t, "on cluster", []any{op.Or("on")}, "on")
	})
	t.Run("Or", func(t *testing.T) {
		checkMatchedSubstr(t, "on cluster", []any{op.Or("cluster")}, "cluster")
	})

	t.Run("Plus - one match", func(t *testing.T) {
		checkMatchedSubstr(t, "a", []any{op.Plus("a")}, "a")
	})
	t.Run("Plus - two matches", func(t *testing.T) {
		checkMatchedSubstr(t, "a a", []any{op.Plus("a")}, "a a")
	})
	t.Run("Plus - zero matches", func(t *testing.T) {
		checkMatchedSubstr(t, "b", []any{op.Plus("a")}, "")
	})
	t.Run("Plus - zero matches", func(t *testing.T) {
		var queryRecurse = []interface{}{
			"recurse",
			"(",
			op.CapturingGroup(op.Plus(op.AnyToken())),
			")",
		}
		originalStr := "RECURSE(gotest)"
		checkCapturingGroups(t, originalStr, queryRecurse, []string{"gotest"})
	})
	t.Run("Plus - one match - complex", func(t *testing.T) {
		checkMatchedSubstr(t, "a b", []any{op.Plus(op.Or("a", "b"))}, "a b")
	})

	t.Run("Seq", func(t *testing.T) {
		checkMatchedSubstr(t, "on cluster", []any{op.Seq("on", "cluster")}, "on cluster")
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
		checkMatchedSubstr(t, "create table qqq on cluster my_cluster() engine=q", queryFull, "create table qqq on cluster my_cluster() engine=q")
	})

	t.Run("capturing group", func(t *testing.T) {
		originalStr := "CREATE TABLE qqq on cluster my_cluster() engine=q"
		checkCapturingGroups(t, originalStr, queryFull, []string{"on cluster my_cluster", "q"})
	})
}
