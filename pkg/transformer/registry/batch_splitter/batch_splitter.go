package batchsplitter

import (
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/transformer"
	"go.ytsaurus.tech/library/go/core/log"
)

const Type = abstract.TransformerType("batch_splitter")

func init() {
	transformer.Register[Config](
		Type,
		func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return &BatchSplitterTransformer{
				config: cfg,
				logger: lgr,
			}, nil
		},
	)
}

type Config struct {
	MaxItemsPerBatch int `json:"max_items_per_batch"`
}

type BatchSplitterTransformer struct {
	config Config
	logger log.Logger
}

func (t *BatchSplitterTransformer) Type() abstract.TransformerType {
	return Type
}

func (t *BatchSplitterTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	return abstract.TransformerResult{
		Transformed: input,
		Errors:      nil,
	}
}

func (t *BatchSplitterTransformer) Suitable(abstract.TableID, *abstract.TableSchema) bool {
	return true
}

func (t *BatchSplitterTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (t *BatchSplitterTransformer) Description() string {
	return "Split one batch into many, useful when changes are heavy or when target throughput is less than source"
}
