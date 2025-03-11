package source

import (
	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers"
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
