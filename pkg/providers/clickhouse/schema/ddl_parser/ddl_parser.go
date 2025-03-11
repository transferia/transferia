package parser

import (
	"github.com/transferria/transferria/pkg/providers/clickhouse/schema/ddl_parser/clickhouse_lexer"
	"github.com/transferria/transferria/pkg/util/token_regexp"
	"github.com/transferria/transferria/pkg/util/token_regexp/abstract"
	"github.com/transferria/transferria/pkg/util/token_regexp/op"
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
	capturingGroups := matchedPath.CapturingGroupArr()
	if len(capturingGroups) != 2 {
		// somehow capturing groups didn't match 2 times
		return "", "", false
	}

	onClusterClause = abstract.ResolveMatchedOps(createDdlSQL, capturingGroups[0])
	engineStr = abstract.ResolveMatchedOps(createDdlSQL, capturingGroups[1])

	return onClusterClause, engineStr, true
}
