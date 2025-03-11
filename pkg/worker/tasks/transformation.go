package tasks

import (
	"context"

	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/util"
)

func AddExtraTransformers(ctx context.Context, transfer *model.Transfer, registry metrics.Registry) error {
	if transformableSource, ok := transfer.Src.(model.ExtraTransformableSource); ok {
		transformers, err := transformableSource.ExtraTransformers(ctx, transfer, registry)
		if err != nil {
			return xerrors.Errorf("cannot set extra transformers from source: %w", err)
		}

		err = util.ForEachErr(transformers, func(transformer abstract.Transformer) error {
			return transfer.AddExtraTransformer(transformer)
		})
		if err != nil {
			return xerrors.Errorf("an error occured during adding a transformation: %w", err)
		}
	}
	return nil
}
