package transformer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

//---------------------------------------------------------------------------------------------------------------------
// simple transformer

func AddTransformer(t *testing.T, transfer *model.Transfer, transformer abstract.Transformer) {
	require.NoError(t, transfer.AddExtraTransformer(transformer))
}

type SimpleTransformerApplyUDF func(*testing.T, []abstract.ChangeItem) abstract.TransformerResult
type SimpleTransformerSuitableUDF func(abstract.TableID, abstract.TableColumns) bool

type SimpleTransformer struct {
	t           *testing.T
	applyUdf    SimpleTransformerApplyUDF
	suitableUdf SimpleTransformerSuitableUDF
}

func (s *SimpleTransformer) Type() abstract.TransformerType {
	return "simple_test_transformer"
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
