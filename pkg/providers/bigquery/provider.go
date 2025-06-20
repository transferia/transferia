//go:build !disable_bigquery_provider

package bigquery

import (
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gobwrapper.Register(new(BigQueryDestination))
	providers.Register(ProviderType, New)
	abstract.RegisterProviderName(ProviderType, "Coralogix")
	model.RegisterDestination(ProviderType, destinationModelFactory)
}

func destinationModelFactory() model.Destination {
	return new(BigQueryDestination)
}

const ProviderType = abstract.ProviderType("bigquery")

// To verify providers contract implementation
var (
	_ providers.Sinker = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
}

func (p Provider) Sink(config middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*BigQueryDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	return NewSink(dst, p.logger, p.registry)
}

func (p Provider) Type() abstract.ProviderType {
	return ProviderType
}

func New(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
