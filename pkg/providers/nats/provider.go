package nats

import (
	"context"
	"encoding/gob"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	cpClient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/providers/nats/source"
	"go.ytsaurus.tech/library/go/core/log"
)

/*
init registers the NatsSource type for gob encoding and associates it with the model and provider framework.
This ensures that the source can be dynamically instantiated and identified by its provider type.
*/
func init() {

	gob.RegisterName("*server.Source", new(source.NatsSource))

	model.RegisterSource(source.ProviderType, func() model.Source {
		return new(source.NatsSource)
	})

	abstract.RegisterProviderName(source.ProviderType, "Nats")

	providers.Register(source.ProviderType, New)
}

/*
Provider struct implements the providers.Provider interface and abstract.Source interface.
*/
type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       cpClient.Coordinator
	transfer *model.Transfer
}

func New(lgr log.Logger, registry metrics.Registry, cp cpClient.Coordinator, transfer *model.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}

func (p *Provider) Type() abstract.ProviderType {
	return source.ProviderType
}

func (p *Provider) Source() (abstract.Source, error) {

	src, ok := p.transfer.Src.(*source.NatsSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}

	return source.New(src.Config, p.logger, p.registry)
}

func (p *Provider) Activate(_ context.Context, _ *model.TransferOperation, _ abstract.TableMap, _ providers.ActivateCallbacks) error {

	if p.transfer.SrcType() == source.ProviderType && !p.transfer.IncrementOnly() {
		return xerrors.New("Only allowed mode for nats source is replication")
	}

	return nil
}
