//go:build !disable_eventhub_provider

package eventhub

import (
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
	gobwrapper.RegisterName("*server.EventHubSource", new(EventHubSource))
	gobwrapper.RegisterName("*server.EventHubAuth", new(EventHubAuth))
	model.RegisterSource(ProviderType, func() model.Source {
		return new(EventHubSource)
	})
	abstract.RegisterProviderName(ProviderType, "Eventhub")
	providers.Register(ProviderType, New)
}

// To verify providers contract implementation
var (
	_ providers.Replication = (*Provider)(nil)
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

func (p *Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*EventHubSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	return NewSource(p.transfer.ID, src, p.logger, p.registry)
}

func New(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
