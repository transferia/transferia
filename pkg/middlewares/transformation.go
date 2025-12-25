package middlewares

import (
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/transformer"
	"go.ytsaurus.tech/library/go/core/log"
)

func Transformation(transfer *model.Transfer, logger log.Logger, metrics metrics.Registry) (func(abstract.Sinker) abstract.Sinker, error) {
	if transfer.HasTransformation() {
		var transformChain []abstract.Transformer
		for _, cfg := range transfer.TransformationConfigs() {
			tr, err := transformer.New(cfg.Type(), cfg.Config(), logger, abstract.TransformationRuntimeOpts{JobIndex: transfer.CurrentJobIndex()})
			if err != nil {
				return nil, xerrors.Errorf("unable to init: %s: %w", cfg.Type(), err)
			}
			transformChain = append(transformChain, tr)
		}
		transformChain = append(transformChain, transfer.Transformation.ExtraTransformers...)
		return transformer.Sinker(
			nil,
			abstract.TransformationRuntimeOpts{JobIndex: transfer.CurrentJobIndex()},
			transformChain,
			logger,
			metrics,
		), nil
	}
	return func(s abstract.Sinker) abstract.Sinker {
		return s
	}, nil
}
