package op

import (
	"github.com/transferia/transferia/pkg/util/token_regexp/abstract"
)

type CapturingGroupOp struct {
	abstract.Relatives
	seq *SeqOp
}

func (t *CapturingGroupOp) IsOp() {}

func (t *CapturingGroupOp) IsCapturingGroup() {}

func (t *CapturingGroupOp) ConsumeComplex(tokens []*abstract.Token) *abstract.MatchedResults {
	result := t.seq.ConsumeComplex(tokens)
	return result
}

func CapturingGroup(in ...any) *CapturingGroupOp {
	result := &CapturingGroupOp{
		Relatives: abstract.NewRelativesImpl(),
		seq:       Seq(in...),
	}
	result.seq.SetParent(result)
	return result
}
