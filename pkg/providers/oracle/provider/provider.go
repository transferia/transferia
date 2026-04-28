package provider

import (
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	providers.Register(provider_oracle.ProviderType, New)
}

var (
	_ providers.Snapshot    = (*Provider)(nil)
	_ providers.Replication = (*Provider)(nil)
	_ providers.Activator   = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry core_metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
}

func (p *Provider) Type() abstract.ProviderType {
	return provider_oracle.ProviderType
}

func (p *Provider) Storage() (abstract.Storage, error) {
	specificConfig, ok := p.transfer.Src.(*provider_oracle.OracleSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	cfg := *specificConfig
	if p.transfer.SnapshotOnly() && cfg.TrackerType == provider_oracle.OracleEmbeddedLogTracker {
		cfg.TrackerType = provider_oracle.OracleInMemoryLogTracker
	}
	return NewOracleStorage(p.logger, p.registry, p.cp, &cfg, p.transfer.ID)
}

func (p *Provider) Source() (abstract.Source, error) {
	specificConfig, ok := p.transfer.Src.(*provider_oracle.OracleSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	return newsource(p.logger, p.registry, p.cp, specificConfig, p.transfer.ID)
}

func New(lgr log.Logger, registry core_metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer, _ *model.TransferOperation) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
