package logger

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer"
	"go.ytsaurus.tech/library/go/core/log"
)

const Type = abstract.TransformerType("logger")

func init() {
	transformer.Register[Config](Type, func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
		return &LoggerTransformer{
			Config: cfg,
			Logger: lgr,
		}, nil
	})
}

type Config struct {
	MaxItemsPerBatch int `json:"max_items_per_batch"`
}

type LoggerTransformer struct {
	Config Config
	Logger log.Logger
}

func (l *LoggerTransformer) Type() abstract.TransformerType {
	return Type
}

func (l *LoggerTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	end := len(input)
	if l.Config.MaxItemsPerBatch > 0 && l.Config.MaxItemsPerBatch <= end {
		end = l.Config.MaxItemsPerBatch
	}

	l.Logger.Info("change items", log.Any("items", input[:end]))
	return abstract.TransformerResult{
		Transformed: input,
		Errors:      nil,
	}
}

func (l *LoggerTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return true
}

func (l *LoggerTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (l *LoggerTransformer) Description() string {
	return "Log the change items"
}
