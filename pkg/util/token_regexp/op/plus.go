package op

import (
	"github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type PlusOp struct {
	abstract.Relatives
	op abstract.Op
}

func (t *PlusOp) IsOp() {}

func consumeComplex(op abstract.Op, tokens []*abstract.Token) *abstract.MatchedResults {
	if len(tokens) == 0 {
		result := abstract.NewMatchedResults()
		return result
	}

	opResult := abstract.NewMatchedResults()

	switch currOp := op.(type) {
	case abstract.OpPrimitive:
		lengths := currOp.ConsumePrimitive(tokens)
		opResult.AddMatchedPathsAfterConsumePrimitive(lengths, op, tokens)
	case abstract.OpComplex:
		localResults := currOp.ConsumeComplex(tokens)
		opResult.AddLocalResults(localResults, op, nil)
	}

	result := abstract.NewMatchedResults()
	for index := range opResult.Size() {
		currOpPath := opResult.Index(index)
		currPathLength := currOpPath.Length()
		localResult := consumeComplex(op, tokens[currPathLength:])

		for localIndex := range localResult.Size() {
			currPath := abstract.NewMatchedPathParentPathChildPath(currOpPath, localResult.Index(localIndex))
			result.AddMatchedPath(currPath)
		}
		result.AddMatchedPath(currOpPath)
	}
	return result
}

func (t *PlusOp) ConsumeComplex(tokens []*abstract.Token) *abstract.MatchedResults {
	return consumeComplex(t.op, tokens)
}

func Plus(arg any) *PlusOp {
	var op abstract.Op = nil

	switch v := arg.(type) {
	case string:
		op = Match(v)
	case abstract.Op:
		op = v
	default:
		return nil
	}

	result := &PlusOp{
		Relatives: abstract.NewRelativesImpl(),
		op:        op,
	}
	result.op.SetParent(result)
	return result
}
