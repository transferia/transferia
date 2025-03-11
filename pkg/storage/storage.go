package storage

import (
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers"
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
