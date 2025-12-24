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

type TransformationRuntimeOpts struct {
	JobIndex int
}

type TransformerType string

type Transformer interface {
	Type() TransformerType
	Description() string
	Suitable(table TableID, schema *TableSchema) bool
	ResultSchema(original *TableSchema) (*TableSchema, error)
	Apply(input []ChangeItem) TransformerResult
}

type TransformerResult struct {
	Transformed []ChangeItem
	Errors      []TransformerError
}

type TransformerError struct {
	Input ChangeItem
	Error error
}
