package op

import (
	"github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type SeqOp struct {
	abstract.Relatives
	ops []abstract.Op
}

func (t *SeqOp) IsOp() {}

func exec(tokens []*abstract.Token, ops []abstract.Op) *abstract.MatchedResults {
	currOp := ops[0]
	opResult := abstract.NewMatchedResults()
	switch v := currOp.(type) {
	case abstract.OpPrimitive:
		lengths := v.ConsumePrimitive(tokens)
		opResult.AddMatchedPathsAfterConsumePrimitive(lengths, currOp, tokens)
	case abstract.OpComplex:
		localResults := v.ConsumeComplex(tokens)
		opResult.AddLocalResults(localResults, currOp, nil)
	}
	if !opResult.IsMatched() {
		return abstract.NewMatchedResults() // NOT FOUND
	}

	leastTempls := ops[1:]
	if len(leastTempls) == 0 { // we successfully executed all op commands
		return opResult
	}

	result := abstract.NewMatchedResults()
	for index := range opResult.Size() {
		currLocalPath := opResult.Index(index)
		localResults := exec(tokens[currLocalPath.Length():], leastTempls)
		result.AddLocalResults(localResults, currOp, tokens[0:currLocalPath.Length()])
	}
	return result
}

func (t *SeqOp) ConsumeComplex(tokens []*abstract.Token) *abstract.MatchedResults {
	return exec(tokens, t.ops)
}

func Seq(in ...any) *SeqOp {
	ops := make([]abstract.Op, 0)
	for _, el := range in {
		switch v := el.(type) {
		case string:
			ops = append(ops, Match(v))
		case abstract.Op:
			ops = append(ops, v)
		default:
			return nil
		}
	}
	result := &SeqOp{
		Relatives: abstract.NewRelativesImpl(),
		ops:       ops,
	}
	for _, childOp := range result.ops {
		childOp.SetParent(result)
	}
	return result
}
