package model

import (
	"encoding/json"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer"
)

type Transformation struct {
	Transformers      *transformer.Transformers
	ExtraTransformers []abstract.Transformer // 'ExtraTransformers' - it's generated in runtime transformers: source-specific transformers; also transformers plugged in tests
	RuntimeJobIndex   int                    // added for some case, which is not in trunk now. Let it be for the future
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

func NewTransformationFromJSON(inJSON string) (*Transformation, error) {
	if inJSON != "{}" && inJSON != "null" {
		transformers := new(transformer.Transformers)
		if err := json.Unmarshal([]byte(inJSON), &transformers); err != nil {
			return nil, xerrors.Errorf("unable to unmarshal transformers: %w", err)
		}
		return &Transformation{
			Transformers:      transformers,
			ExtraTransformers: nil,
			RuntimeJobIndex:   0,
		}, nil
	}
	return nil, nil
}
