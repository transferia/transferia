package abstract

import (
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

var (
	transformersLock sync.Mutex
	transformers     = make(map[string]SinkOption)
)

func AddTransformer(transformerName string, transformer SinkOption) error {
	transformersLock.Lock()
	defer transformersLock.Unlock()

	if _, ok := transformers[transformerName]; ok {
		return xerrors.Errorf("transformer added more than once: %s", transformerName)
	}

	transformers[transformerName] = transformer
	return nil
}

func GetTransformers(middlewareNames []string) ([]SinkOption, error) {
	transformersLock.Lock()
	defer transformersLock.Unlock()

	result := make([]SinkOption, 0)
	for _, currTransformerName := range middlewareNames {
		if transformer, ok := transformers[currTransformerName]; ok {
			result = append(result, transformer)
		} else {
			return nil, xerrors.Errorf("unknown transformer name: %s", currTransformerName)
		}
	}
	return result, nil
}

type TransformationRuntimeOpts struct {
	JobIndex int
}

type Transformation interface {
	MakeSinkMiddleware() SinkOption
	AddTransformer(transformer Transformer) error
	RuntimeOpts() TransformationRuntimeOpts
}

type TransformerType string

type Transformer interface {
	Apply(input []ChangeItem) TransformerResult
	Suitable(table TableID, schema *TableSchema) bool
	ResultSchema(original *TableSchema) (*TableSchema, error)
	Description() string
	Type() TransformerType
}

type TransformerResult struct {
	Transformed []ChangeItem
	Errors      []TransformerError
}

type TransformerError struct {
	Input ChangeItem
	Error error
}
