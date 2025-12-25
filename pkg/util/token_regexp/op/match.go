package op

import (
	"strings"

	"github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type MatchOp struct {
	abstract.Relatives
	v string
}

func (t *MatchOp) IsOp() {}

func (t *MatchOp) ConsumePrimitive(tokens []*abstract.Token) []int {
	if len(tokens) == 0 {
		return nil
	}
	if t.v == tokens[0].LowerText {
		return []int{1}
	}
	return nil
}

func Match(in string) *MatchOp {
	return &MatchOp{
		Relatives: abstract.NewRelativesImpl(),
		v:         strings.ToLower(in),
	}
}
