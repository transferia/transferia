package data

import (
	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/base"
	"github.com/transferria/transferria/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	TryLegacySourceError = xerrors.New("Not found in new sources, try legacy source")
)

func NewDataProvider(lgr log.Logger, registry metrics.Registry, transfer *model.Transfer, cp coordinator.Coordinator) (p base.DataProvider, err error) {
	dataer, ok := providers.Source[providers.Abstract2Provider](lgr, registry, cp, transfer)
	if !ok {
		return nil, TryLegacySourceError
	}
	res, err := dataer.DataProvider()
	if err == TryLegacySourceError {
		return nil, TryLegacySourceError
	}
	if err != nil {
		return nil, xerrors.Errorf("unable to create %T: %w", transfer.Src, err)
	}
	return res, nil
}

func NewSnapshotProvider(lgr log.Logger, registry metrics.Registry, transfer *model.Transfer, cp coordinator.Coordinator) (base.SnapshotProvider, error) {
	p, err := NewDataProvider(lgr, registry, transfer, cp)
	if err != nil {
		return nil, xerrors.Errorf("unable to get data provider: %w", err)
	}

	sp, ok := p.(base.SnapshotProvider)
	if !ok {
		return nil, xerrors.Errorf("data provider for this source does not support snapshot")
	}
	return sp, nil
}
