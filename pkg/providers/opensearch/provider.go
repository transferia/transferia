//go:build !disable_opensearch_provider

package opensearch

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/providers/elastic"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gobwrapper.RegisterName("*server.OpenSearchDestination", new(OpenSearchDestination))
	gobwrapper.RegisterName("*server.OpenSearchSource", new(OpenSearchSource))

	abstract.RegisterProviderName(ProviderType, "OpenSearch")

	model.RegisterDestination(ProviderType, destinationModelFactory)
	model.RegisterSource(ProviderType, func() model.Source {
		return new(OpenSearchSource)
	})

	providers.Register(ProviderType, New)
}

func destinationModelFactory() model.Destination {
	return new(OpenSearchDestination)
}

const ProviderType = abstract.ProviderType("opensearch")

// To verify providers contract implementation
var (
	_ providers.Sinker    = (*Provider)(nil)
	_ providers.Snapshot  = (*Provider)(nil)
	_ providers.Activator = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p *Provider) Activate(ctx context.Context, task *model.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	if !p.transfer.SnapshotOnly() {
		return abstract.NewFatalError(xerrors.Errorf("only snapshot mode is allowed for the Opensearch source"))
	}
	if err := callbacks.Cleanup(tables); err != nil {
		return xerrors.Errorf("failed to cleanup sink: %w", err)
	}
	if err := callbacks.CheckIncludes(tables); err != nil {
		return xerrors.Errorf("failed in accordance with configuration: %w", err)
	}
	if err := elastic.DumpIndexInfo(p.transfer, p.logger, p.registry); err != nil {
		return xerrors.Errorf("failed to dump source indexes info: %w", err)
	}
	if err := callbacks.Upload(tables); err != nil {
		return xerrors.Errorf("transfer (snapshot) failed: %w", err)
	}
	return nil
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*OpenSearchSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	if _, ok := p.transfer.Dst.(elastic.IsElasticLikeDestination); ok {
		result, err := NewStorage(src, p.logger, p.registry, elastic.WithHomo())
		if err != nil {
			return nil, xerrors.Errorf("unable to create storage with ElasticLike dst, err: %w", err)
		}
		return result, nil
	}
	return NewStorage(src, p.logger, p.registry)
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*OpenSearchDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	return NewSink(dst, p.logger, p.registry)
}

func New(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
