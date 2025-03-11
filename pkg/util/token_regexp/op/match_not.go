package op

import (
	"strings"

	"github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type MatchNotOp struct {
	abstract.Relatives
	v map[string]bool
}

func (t *MatchNotOp) IsOp() {}

func (t *MatchNotOp) ConsumePrimitive(tokens []*abstract.Token) []int {
	if len(tokens) == 0 {
		return nil
	}
	if !t.v[tokens[0].LowerText] {
		return []int{1}
	}
	return nil
}

func MatchNot(in ...string) *MatchNotOp {
	v := make(map[string]bool)
	for _, vv := range in {
		v[strings.ToLower(vv)] = true
	}
	return &MatchNotOp{
		Relatives: abstract.NewRelativesImpl(),
		v:         v,
	}
}
