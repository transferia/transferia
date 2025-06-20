//go:build !disable_s3_provider

package provider

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/providers/s3"
	_ "github.com/transferia/transferia/pkg/providers/s3/fallback"
	s3_sink "github.com/transferia/transferia/pkg/providers/s3/sink"
	"github.com/transferia/transferia/pkg/providers/s3/source"
	objectfetcher "github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher"
	"github.com/transferia/transferia/pkg/providers/s3/storage"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	providers.Register(s3.ProviderType, New)
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
	registry metrics.Registry
	cp       cpclient.Coordinator
	transfer *model.Transfer
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
		srcModel, ok := p.transfer.Src.(*s3.S3Source)
		if !ok {
			return xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
		}
		runtimeStub := abstract.NewFakeShardingTaskRuntime(0, 1, 1, 1)
		if objectfetcher.DeriveObjectFetcherType(srcModel) == objectfetcher.Poller {
			err := objectfetcher.FetchAndCommit(ctx, srcModel, p.transfer.ID, p.logger, p.registry, p.cp, runtimeStub, false)
			if err != nil {
				return xerrors.Errorf("Failed to fetch and commit: %w", err)
			}
		}
	}
	return nil
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*s3.S3Source)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	return storage.New(src, p.logger, p.registry)
}

func (p *Provider) Type() abstract.ProviderType {
	return s3.ProviderType
}

func (p *Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*s3.S3Source)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	shardingRuntime, ok := p.transfer.Runtime.(abstract.ShardingTaskRuntime)
	if !ok {
		return nil, xerrors.Errorf("s3 source not supported non-sharding runtime: %T", p.transfer.Runtime)
	}
	return source.NewSource(src, p.transfer.ID, p.logger, p.registry, p.cp, shardingRuntime)
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*s3.S3Destination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	return s3_sink.NewSink(p.logger, dst, p.registry, p.cp, p.transfer.ID)
}

func New(lgr log.Logger, registry metrics.Registry, cp cpclient.Coordinator, transfer *model.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
