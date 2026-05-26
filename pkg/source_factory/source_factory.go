package source_factory

import (
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewSource(transfer *model.Transfer, lgr log.Logger, registry core_metrics.Registry, cp coordinator.Coordinator) (abstract.Source, error) {
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

func NewPartitionableSource(transfer *model.Transfer, lgr log.Logger, registry core_metrics.Registry, cp coordinator.Coordinator, partition abstract.Partition) (abstract.QueueToS3Source, error) {
	replicator, ok := providers.Source[providers.PartitionableSource](lgr, registry, cp, transfer)
	if !ok {
		lgr.Error("Unable to create partitionable source")
		return nil, xerrors.Errorf("unknown source: %s: %T", transfer.SrcType(), transfer.Src)
	}
	res, err := replicator.PartitionSource(partition)
	if err != nil {
		return nil, xerrors.Errorf("unable to create async %T: %w", transfer.Src, err)
	}
	return res, nil
}

func NewPartitionLister(transfer *model.Transfer, lgr log.Logger, registry core_metrics.Registry, cp coordinator.Coordinator) (abstract.PartitionLister, error) {
	listerProvider, ok := providers.Source[providers.PartitionListerProvider](lgr, registry, cp, transfer)
	if !ok {
		lgr.Error("Unable to create partition lister")
		return nil, xerrors.Errorf("source does not support partition listing: %s: %T", transfer.SrcType(), transfer.Src)
	}
	res, err := listerProvider.PartitionLister()
	if err != nil {
		return nil, xerrors.Errorf("unable to create partition lister for %T: %w", transfer.Src, err)
	}
	return res, nil
}
