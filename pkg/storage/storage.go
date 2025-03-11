package storage

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
)

var UnsupportedSourceErr = xerrors.New("Unsupported storage")

func NewStorage(transfer *model.Transfer, cp coordinator.Coordinator, registry metrics.Registry) (abstract.Storage, error) {
	switch src := transfer.Src.(type) {
	case *model.MockSource:
		return src.StorageFactory(), nil
	default:
		snapshoter, ok := providers.Source[providers.Snapshot](logger.Log, registry, cp, transfer)
		if !ok {
			return nil, xerrors.Errorf("%w: %s: %T", UnsupportedSourceErr, transfer.SrcType(), transfer.Src)
		}
		res, err := snapshoter.Storage()
		if err != nil {
			return nil, xerrors.Errorf("unable to create %T: %w", transfer.Src, err)
		}
		return res, nil
	}
}
