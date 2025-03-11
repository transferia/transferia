package source

import (
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSource(transfer *model.Transfer, lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator) (abstract.Source, error) {
	replicator, ok := providers.Source[providers.Replication](lgr, registry, cp, transfer)
	if !ok {
		lgr.Error("Unable to create source")
		return nil, xerrors.Errorf("unknown source: %s: %T", transfer.SrcType(), transfer.Src)
	}
	res, err := replicator.Source()
	if err != nil {
		return nil, xerrors.Errorf("unable to create %T: %w", transfer.Src, err)
	}
	return res, nil
}
