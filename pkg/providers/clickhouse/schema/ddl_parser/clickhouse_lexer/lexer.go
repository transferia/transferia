//go:build !disable_clickhouse_provider

package clickhouse_lexer

import "github.com/antlr4-go/antlr/v4"

func StringToTokens(in string) []antlr.Token {
	input := antlr.NewInputStream(in)
	lexer := NewClickHouseLexer(input)

	tokens := make([]antlr.Token, 0)

	for {
		t := lexer.NextToken()
		if t.GetTokenType() == antlr.TokenEOF {
			break
		}
		tokens = append(tokens, t)
	}

	return tokens
}
