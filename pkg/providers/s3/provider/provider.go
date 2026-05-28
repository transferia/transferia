package provider

import (
	"context"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	_ "github.com/transferia/transferia/pkg/providers/s3/fallback"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher"
	s3_sink "github.com/transferia/transferia/pkg/providers/s3/sink"
	s3_source "github.com/transferia/transferia/pkg/providers/s3/source"
	s3_storage "github.com/transferia/transferia/pkg/providers/s3/storage"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	providers.Register(s3_model.ProviderType, New)
}

// To verify providers contract implementation
var (
	_ providers.Sinker      = (*Provider)(nil)
	_ providers.Snapshot    = (*Provider)(nil)
	_ providers.Activator   = (*Provider)(nil)
	_ providers.Replication = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry core_metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer

	operation *model.TransferOperation
}

func (p *Provider) Activate(ctx context.Context, task *model.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	if !p.transfer.IncrementOnly() {
		if err := callbacks.Cleanup(tables); err != nil {
			return xerrors.Errorf("Sink cleanup failed: %w", err)
		}
		if err := callbacks.CheckIncludes(tables); err != nil {
			return xerrors.Errorf("Failed in accordance with configuration: %w", err)
		}
		if err := callbacks.Upload(tables); err != nil {
			return xerrors.Errorf("Snapshot loading failed: %w", err)
		}
	} else {
		// if increment only
		srcModel, ok := p.transfer.Src.(*s3_model.S3Source)
		if !ok {
			return xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
		}
		if object_fetcher.DeriveObjectFetcherType(srcModel) == object_fetcher.Poller {
			err := object_fetcher.FetchAndCommit(ctx, srcModel, p.transfer.ID, p.logger, p.registry, p.cp)
			if err != nil {
				return xerrors.Errorf("Failed to fetch and commit: %w", err)
			}
		}
	}
	return nil
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*s3_model.S3Source)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	return s3_storage.New(src, p.transfer.ID, p.logger, p.registry)
}

func (p *Provider) Type() abstract.ProviderType {
	return s3_model.ProviderType
}

func (p *Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*s3_model.S3Source)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	shardingRuntime, ok := p.transfer.RuntimeForReplication().(abstract.ShardingTaskRuntime)
	if !ok {
		return nil, xerrors.Errorf("s3 source not supported non-sharding runtime: %T", p.transfer.Runtime)
	}
	return s3_source.NewSource(src, p.transfer.ID, p.logger, p.registry, p.cp, shardingRuntime)
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*s3_model.S3Destination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}

	switch p.transfer.Type {
	case abstract.TransferTypeSnapshotOnly:
		if p.operation == nil {
			return nil, xerrors.Errorf("operation is nil")
		}
		sink, err := s3_sink.NewSnapshotSink(p.logger, dst, p.registry, p.cp, p.transfer.ID, p.operation.CreatedAt.Unix())
		if err != nil {
			return nil, xerrors.Errorf("failed to create snapshot sink: %w", err)
		}
		return sink, nil
	case abstract.TransferTypeIncrementOnly:
		sink, err := s3_sink.NewReplicationSink(p.logger, dst, p.registry, p.cp, p.transfer.ID)
		if err != nil {
			return nil, xerrors.Errorf("failed to create replication sink: %w", err)
		}
		return sink, nil
	default:
		return nil, xerrors.Errorf("unsupported transfer type: %v", p.transfer.Type)
	}
}

func New(lgr log.Logger, registry core_metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer, operation *model.TransferOperation) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,

		operation: operation,
	}
}
