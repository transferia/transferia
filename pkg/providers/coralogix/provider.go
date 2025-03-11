package coralogix

import (
	"encoding/gob"

	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/middlewares"
	"github.com/transferria/transferria/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gob.Register(new(CoralogixDestination))
	providers.Register(ProviderType, New)
	abstract.RegisterProviderName(ProviderType, "Coralogix")
	model.RegisterDestination(ProviderType, destinationModelFactory)
}

func destinationModelFactory() model.Destination {
	return new(CoralogixDestination)
}

const ProviderType = abstract.ProviderType("coralogix")

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
	dst, ok := p.transfer.Dst.(*CoralogixDestination)
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
