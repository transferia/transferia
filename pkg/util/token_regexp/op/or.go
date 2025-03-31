package op

import (
	"github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type OrOp struct {
	abstract.Relatives
	ops []abstract.Op
}

func (t *OrOp) IsOp() {}

func (t *OrOp) ConsumeComplex(tokens []*abstract.Token) *abstract.MatchedResults {
	result := abstract.NewMatchedResults()
	for index := range t.ops {
		switch currOp := t.ops[index].(type) {
		case abstract.OpPrimitive:
			lengths := currOp.ConsumePrimitive(tokens)
			result.AddMatchedPathsAfterConsumePrimitive(lengths, t.ops[index], tokens)
		case abstract.OpComplex:
			localResults := currOp.ConsumeComplex(tokens)
			result.AddLocalResults(localResults, t.ops[index], nil)
		}
	}
	return result
}

func Or(args ...any) *OrOp {
	resultArgs := make([]abstract.Op, 0, len(args))
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			resultArgs = append(resultArgs, Match(v))
		case abstract.Op:
			resultArgs = append(resultArgs, v)
		default:
			return nil
		}
	}
	result := &OrOp{
		Relatives: abstract.NewRelativesImpl(),
		ops:       resultArgs,
	}
	for _, childOp := range result.ops {
		childOp.SetParent(result)
	}
	return result
}
