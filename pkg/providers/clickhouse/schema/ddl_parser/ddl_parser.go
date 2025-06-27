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

//---

var engineQueryFull = []interface{}{
	op.CapturingGroup(
		op.AnyToken(),
		op.Opt(op.MatchParentheses()),
	),
}

func ExtractEngine(engineSQL string) (string, string, []string, bool) {
	tokens := clickhouse_lexer.StringToTokens(engineSQL)
	currMatcher := token_regexp.NewTokenRegexp(engineQueryFull)
	results := currMatcher.FindAll(tokens)
	if results.Size() == 0 {
		// not matched
		return "", "", nil, false
	}
	matchedPath := results.Index(0) // take the longest match
	capturingGroups := matchedPath.CapturingGroups()
	if capturingGroups.GroupsNum() != 1 {
		// somehow capturing groups didn't match 1 times
		return "", "", nil, false
	}

	resultEngineStr := capturingGroups.GroupToText(engineSQL, 0)

	paramTokens := clickhouse_lexer.StringToTokens(resultEngineStr)
	paramTokensStrArr := make([]string, 0, len(paramTokens))
	for _, token := range paramTokens {
		paramTokensStrArr = append(paramTokensStrArr, token.GetText())
	}
	engineName := paramTokensStrArr[0]
	paramTokensStrArr = paramTokensStrArr[1:]
	result := make([]string, 0, len(paramTokensStrArr))
	if len(paramTokensStrArr) >= 2 {
		paramTokensStrArr = paramTokensStrArr[1 : len(paramTokensStrArr)-1]
		for i := 0; i < len(paramTokensStrArr); i++ {
			if i%2 == 0 {
				result = append(result, paramTokensStrArr[i])
			}
		}
	}

	return resultEngineStr, engineName, result, true
}

//---

func ExtractEngineStrEngineParams(createDdlSQL string) (string, string, []string, bool) {
	_, engineStr, found := ExtractNameClusterEngine(createDdlSQL)
	if !found {
		return "", "", nil, false
	}
	resultEngineStr, engineName, params, isFound := ExtractEngine(engineStr)
	return resultEngineStr, engineName, params, isFound
}
