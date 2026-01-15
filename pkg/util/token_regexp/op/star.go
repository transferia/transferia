package op

import "github.com/transferia/transferia/pkg/util/token_regexp/abstract"

type StarOp struct {
	abstract.Relatives
	op abstract.Op
}

func (t *StarOp) IsOp() {}
func (t *StarOp) ConsumeComplex(tokens []*abstract.Token) *abstract.MatchedResults {
	result := consumeComplex(t.op, tokens)
	result.AddMatchedPath(abstract.NewMatchedPathEmpty())
	return result
}

func Star(arg any) *StarOp {
	var op abstract.Op = nil
	switch v := arg.(type) {
	case string:
		op = Match(v)
	case abstract.Op:
		op = v
	default:
		return nil
	}
	result := &StarOp{
		Relatives: abstract.NewRelativesImpl(),
		op:        op,
	}
	result.op.SetParent(result)
	return result
}
