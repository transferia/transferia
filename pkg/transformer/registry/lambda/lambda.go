package lambda

import (
	"fmt"

	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/functions"
	"github.com/transferria/transferria/pkg/transformer"
	"go.ytsaurus.tech/library/go/core/log"
)

const TransformerType = abstract.TransformerType("lambda")

func init() {
	transformer.Register[Config](
		TransformerType,
		func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
			return New(cfg, lgr), nil
		},
	)
}

type Config struct {
	TableID abstract.TableID
	Options *model.DataTransformOptions
}

type mapper struct {
	table    abstract.TableID
	cfg      *model.DataTransformOptions
	lgr      log.Logger
	executor *functions.Executor
}

func (m *mapper) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	res, err := m.executor.Do(input)
	if err != nil {
		return abstract.TransformerResult{
			Transformed: nil,
			Errors:      allWithError(input, err),
		}
	}
	return abstract.TransformerResult{
		Transformed: res,
		Errors:      nil,
	}
}

func (m *mapper) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return m.table == table
}

func (m *mapper) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	executor, err := functions.NewExecutor(m.cfg, m.cfg.CloudFunctionsBaseURL, functions.CDC, m.lgr, solomon.NewRegistry(solomon.NewRegistryOpts()))
	if err != nil {
		return nil, xerrors.Errorf("unable to init functions transformer: %w", err)
	}
	m.executor = executor
	res, err := executor.Do([]abstract.ChangeItem{})
	if err != nil {
		return nil, xerrors.Errorf("unable to check test change item: %w", err)
	}
	if len(res) > 0 {
		return res[0].TableSchema, nil
	}
	return original, nil
}

func (m *mapper) Description() string {
	return fmt.Sprintf(`lamdba function: %s`, m.cfg.CloudFunction)
}

func (m *mapper) Type() abstract.TransformerType {
	return TransformerType
}

func allWithError(input []abstract.ChangeItem, err error) []abstract.TransformerError {
	res := make([]abstract.TransformerError, len(input))
	for i, row := range input {
		res[i] = abstract.TransformerError{
			Input: row,
			Error: err,
		}
	}
	return res
}

func New(cfg Config, lgr log.Logger) abstract.Transformer {
	return &mapper{
		table:    cfg.TableID,
		cfg:      cfg.Options,
		lgr:      lgr,
		executor: nil,
	}
}
