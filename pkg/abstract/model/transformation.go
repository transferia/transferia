package model

import (
	"encoding/json"

	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/library/go/core/xerrors/multierr"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/transformer"
)

type Transformation struct {
	Transformers      *transformer.Transformers
	ExtraTransformers []abstract.Transformer
	Executor          abstract.Transformation
	RuntimeJobIndex   int
}

func (t Transformation) Validate() error {
	if t.Transformers == nil {
		return nil
	}
	var errs error
	for _, tr := range t.Transformers.Transformers {
		_, err := transformer.New(tr.Type(), tr.Config(), logger.Log, abstract.TransformationRuntimeOpts{JobIndex: 0})
		if err != nil {
			errs = multierr.Append(errs, xerrors.Errorf("unable to construct: %s(%s): %w", tr.Type(), tr.ID(), err))
		}
	}
	if errs != nil {
		return xerrors.Errorf("transformers invalid: %w", errs)
	}
	return nil
}

func MakeTransformationFromJSON(value string) (*Transformation, error) {
	if value != "{}" && value != "null" {
		result := new(Transformation)
		trs := new(transformer.Transformers)
		if err := json.Unmarshal([]byte(value), &trs); err != nil {
			return nil, xerrors.Errorf("unable to unmarshal transformers: %w", err)
		}
		result.Transformers = trs
		return result, nil
	}
	return nil, nil
}
