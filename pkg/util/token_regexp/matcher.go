package token_regexp

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/transferria/transferria/pkg/util/token_regexp/abstract"
	"github.com/transferria/transferria/pkg/util/token_regexp/op"
)

type TokenRegexp struct {
	ops []abstract.Op
}

// FindAll - just like in package 'regexp', method Regexp.FindAll
func (m *TokenRegexp) FindAll(tokens []antlr.Token) *abstract.MatchedResults {
	matcherTokens := make([]*abstract.Token, 0, len(tokens))
	for _, currToken := range tokens {
		matcherTokens = append(matcherTokens, abstract.NewToken(currToken))
	}

	args := make([]any, 0, len(m.ops))
	for _, el := range m.ops {
		args = append(args, el)
	}
	seq := op.Seq(args...)

	result := abstract.NewMatchedResults()
	for index := range matcherTokens {
		currResult := seq.ConsumeComplex(matcherTokens[index:])
		result.AddAllPaths(currResult)
	}
	return result
}

// NewTokenRegexp - ctor for 'TokenRegexp' object
//   - expr - can be either Op (see 'op' package) or `string` (means op.Match - just to improve readability)
func NewTokenRegexp(expr []any) *TokenRegexp {
	ops := make([]abstract.Op, 0, len(expr))
	for _, el := range expr {
		switch v := el.(type) {
		case string:
			ops = append(ops, op.Match(v))
		case abstract.OpPrimitive:
			ops = append(ops, v)
		case abstract.OpComplex:
			ops = append(ops, v)
		default:
			return nil
		}
	}
	return &TokenRegexp{
		ops: ops,
	}
}
