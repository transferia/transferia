package helpers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	_ "github.com/transferia/transferia/pkg/dataplane"
)

//---------------------------------------------------------------------------------------------------------------------
// useful addition of transformer to transfer

func AddTransformer(t *testing.T, transfer *model.Transfer, transformer abstract.Transformer) {
	err := transfer.AddExtraTransformer(transformer)
	require.NoError(t, err)
}

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
