package op

import (
	"github.com/transferria/transferria/pkg/util/token_regexp/abstract"
)

type SeqOp struct {
	abstract.Relatives
	ops []abstract.Op
}

func (t *SeqOp) IsOp() {}

func exec(tokens []*abstract.Token, ops []abstract.Op) *abstract.MatchedResults {
	currOp := ops[0]
	oneElementResult := abstract.NewMatchedResults()
	switch v := currOp.(type) {
	case abstract.OpPrimitive:
		lengths := v.ConsumePrimitive(tokens)
		oneElementResult.AddMatchedPathsAfterSimpleConsume(lengths, currOp, tokens)
	case abstract.OpComplex:
		oneElementResult = v.ConsumeComplex(tokens)
		oneElementResult.AddMatchedPathsAfterConsumeComplex(currOp)
	}
	if !oneElementResult.IsMatched() {
		return abstract.NewMatchedResults() // NOT FOUND
	}

	leastTempls := ops[1:]
	if len(leastTempls) == 0 { // we successfully executed all op commands
		return oneElementResult
	}

	globalResult := abstract.NewMatchedResults()
	for index := range oneElementResult.Size() {
		currLocalPath := oneElementResult.Index(index)
		currResults := exec(tokens[currLocalPath.Length():], leastTempls)
		globalResult.AddLocalPaths(currResults, currOp, tokens[0:currLocalPath.Length()])
	}
	return globalResult
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
