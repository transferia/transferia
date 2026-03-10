package problemitemdetector

import (
	"github.com/transferia/transferia/pkg/abstract"
	transformerregistry "github.com/transferia/transferia/pkg/transformer"
	"go.ytsaurus.tech/library/go/core/log"
)

type Config struct{}

func init() {
	transformerregistry.Register[Config](TransformerType, func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
		return &problemItemDetector{cfg: cfg, logger: lgr}, nil
	})
}

type problemItemDetector struct {
	cfg    Config
	logger log.Logger
}

var _ abstract.Transformer = (*problemItemDetector)(nil)

const TransformerType = abstract.TransformerType("problem_item_detector")

func (b problemItemDetector) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	return abstract.TransformerResult{
		Transformed: input,
		Errors:      nil,
	}
}

func (b problemItemDetector) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return true
}

func (b problemItemDetector) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (b problemItemDetector) Description() string {
	return "problem item detector"
}

func (b problemItemDetector) Type() abstract.TransformerType {
	return TransformerType
}
