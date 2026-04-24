package provider

import (
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/providers"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	providers.Register(provider_oracle.ProviderType, New)
}

// To verify providers contract implementation
var (
	_ providers.Abstract2Provider = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry core_metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
}

func (p *Provider) DataProvider() (abstract2.DataProvider, error) {
	specificConfig, ok := p.transfer.Src.(*provider_oracle.OracleSource)
	if !ok {
		return nil, xerrors.Errorf("Unexpected source type: %T", p.transfer.Src)
	}
	// for snapshot only transfers there is no need to tracker
	// we should set in-memory if embedded enabled
	// this allows us to use RO connection for snapshot only transfer
	if p.transfer.SnapshotOnly() && specificConfig.TrackerType == provider_oracle.OracleEmbeddedLogTracker {
		specificConfig.TrackerType = provider_oracle.OracleInMemoryLogTracker
	}
	return NewOracleDataProvider(p.logger, p.registry, p.cp, specificConfig, p.transfer.ID)
}

func (p *Provider) Type() abstract.ProviderType {
	return provider_oracle.ProviderType
}

func New(lgr log.Logger, registry core_metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer, _ *model.TransferOperation) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
