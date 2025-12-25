package op

import (
	"github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type OptOp struct {
	abstract.Relatives
	op abstract.Op
}

func (t *OptOp) IsOp() {}

func (t *OptOp) ConsumeComplex(tokens []*abstract.Token) *abstract.MatchedResults {
	result := abstract.NewMatchedResults()
	if len(tokens) == 0 {
		result.AddMatchedPath(abstract.NewMatchedPathEmpty())
		return result
	}
	switch v := t.op.(type) {
	case abstract.OpPrimitive:
		lengths := v.ConsumePrimitive(tokens)
		result.AddMatchedPathsAfterConsumePrimitive(lengths, t.op, tokens)
	case abstract.OpComplex:
		localResults := v.ConsumeComplex(tokens)
		result.AddLocalResults(localResults, t.op, nil)
	}
	result.AddMatchedPath(abstract.NewMatchedPathEmpty())
	return result
}

func Opt(in any) *OptOp {
	var result *OptOp = nil
	switch v := in.(type) {
	case string:
		result = &OptOp{
			Relatives: abstract.NewRelativesImpl(),
			op:        Match(v),
		}
	case abstract.Op:
		result = &OptOp{
			Relatives: abstract.NewRelativesImpl(),
			op:        v,
		}
	default:
		return nil
	}
	result.op.SetParent(result)
	return result
}
