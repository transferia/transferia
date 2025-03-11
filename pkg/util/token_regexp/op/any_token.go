package op

import (
	"github.com/transferria/transferria/pkg/util/token_regexp/abstract"
)

type AnyTokenOp struct {
	abstract.Relatives
}

func (t *AnyTokenOp) IsOp() {}

func (t *AnyTokenOp) ConsumePrimitive(tokens []*abstract.Token) []int {
	if len(tokens) == 0 {
		return nil
	}
	return []int{1}
}

func AnyToken() *AnyTokenOp {
	return &AnyTokenOp{
		Relatives: abstract.NewRelativesImpl(),
	}
}
