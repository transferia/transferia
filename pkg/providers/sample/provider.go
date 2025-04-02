package sample

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gobwrapper.Register(new(SampleSource))
	model.RegisterSource(ProviderType, func() model.Source {
		return new(SampleSource)
	})
	abstract.RegisterProviderName(ProviderType, "Sample")
	providers.Register(ProviderType, func(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) providers.Provider {
		return &Provider{
			logger:   lgr,
			registry: registry,
			cp:       cp,
			transfer: transfer,
		}
	})
}

const ProviderType = abstract.ProviderType("sample")

var (
	_ providers.Replication = (*Provider)(nil)
	_ providers.Snapshot    = (*Provider)(nil)
	_ providers.Activator   = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
}

func (p *Provider) Activate(ctx context.Context, task *model.TransferOperation, table abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	_, ok := p.transfer.Src.(*SampleSource)
	if !ok {
		return xerrors.Errorf("unexpected source: %T", p.transfer.Src)
	}
	if !p.transfer.IncrementOnly() {
		if err := callbacks.Upload(table); err != nil {
			return xerrors.Errorf("Snapshot loading failed: %w", err)
		}
	}

	return nil
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*SampleSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}

	return NewStorage(src, p.logger)
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p *Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*SampleSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}

	return NewSource(src, p.transfer.ID, p.logger, p.registry)
}
