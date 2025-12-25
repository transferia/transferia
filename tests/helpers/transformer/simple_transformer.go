package transformer

import (
	"testing"

	"github.com/transferia/transferia/pkg/abstract"
	_ "github.com/transferia/transferia/pkg/dataplane"
)

//---------------------------------------------------------------------------------------------------------------------
// simple transformer

type SimpleTransformerApplyUDF func(*testing.T, []abstract.ChangeItem) abstract.TransformerResult
type SimpleTransformerSuitableUDF func(abstract.TableID, abstract.TableColumns) bool

type SimpleTransformer struct {
	t           *testing.T
	applyUdf    SimpleTransformerApplyUDF
	suitableUdf SimpleTransformerSuitableUDF
}

func (s *SimpleTransformer) Type() abstract.TransformerType {
	return abstract.TransformerType("simple_test_transformer")
}

func (s *SimpleTransformer) Apply(items []abstract.ChangeItem) abstract.TransformerResult {
	return s.applyUdf(s.t, items)
}

func (s *SimpleTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return s.suitableUdf(table, schema.Columns())
}

func (s *SimpleTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (s *SimpleTransformer) Description() string {
	return "SimpleTransformer for tests"
}

func NewSimpleTransformer(t *testing.T, applyUdf SimpleTransformerApplyUDF, suitableUdf SimpleTransformerSuitableUDF) *SimpleTransformer {
	return &SimpleTransformer{
		t:           t,
		applyUdf:    applyUdf,
		suitableUdf: suitableUdf,
	}
}
