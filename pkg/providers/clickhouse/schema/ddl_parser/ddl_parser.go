package parser

import (
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema/ddl_parser/clickhouse_lexer"
	"github.com/transferia/transferia/pkg/util/token_regexp"
	"github.com/transferia/transferia/pkg/util/token_regexp/op"
)

var queryFull = []interface{}{
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

func ExtractNameClusterEngine(createDdlSQL string) (onClusterClause, engineStr string, found bool) {
	tokens := clickhouse_lexer.StringToTokens(createDdlSQL)
	currMatcher := token_regexp.NewTokenRegexp(queryFull)
	results := currMatcher.FindAll(tokens)
	if results.Size() == 0 {
		// not matched
		return "", "", false
	}
	matchedPath := results.Index(0) // take the longest match
	capturingGroups := matchedPath.CapturingGroups()
	if capturingGroups.GroupsNum() != 2 {
		// somehow capturing groups didn't match 2 times
		return "", "", false
	}

	onClusterClause = capturingGroups.GroupToText(createDdlSQL, 0)
	engineStr = capturingGroups.GroupToText(createDdlSQL, 1)

	return onClusterClause, engineStr, true
}
