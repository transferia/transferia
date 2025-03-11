package dbt

import (
	"fmt"

	"github.com/transferria/transferria/pkg/abstract"
	transformerregistry "github.com/transferria/transferria/pkg/transformer"
	"go.ytsaurus.tech/library/go/core/log"
)

type Config struct {
	ProfileName       string
	GitBranch         string
	GitRepositoryLink string
	Operation         string
}

func init() {
	transformerregistry.Register[Config](TransformerType, func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
		return &dbt{cfg: cfg}, nil
	})
}

type dbt struct {
	cfg Config
}

var _ abstract.Transformer = (*dbt)(nil)

const TransformerType = abstract.TransformerType("dbt")

func (t *dbt) Type() abstract.TransformerType {
	return TransformerType
}

func (t *dbt) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	return abstract.TransformerResult{
		Transformed: input,
		Errors:      nil,
	}
}

func (t *dbt) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return false
}

func (t *dbt) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (t *dbt) Description() string {
	return fmt.Sprintf("DBT: %s", t.cfg.GitRepositoryLink)
}
