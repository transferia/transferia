package kinesis

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	gobwrapper.Register(new(KinesisSource))
	model.RegisterSource(ProviderType, func() model.Source {
		return new(KinesisSource)
	})
	abstract.RegisterProviderName(ProviderType, "Kinesis")

	providers.Register(ProviderType, New)
}

const ProviderType = abstract.ProviderType("kinesis")

var (
	_ providers.Replication = (*Provider)(nil)

	_ providers.Activator = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       cpclient.Coordinator
	transfer *model.Transfer
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p *Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*KinesisSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected source type: %T", p.transfer.Src)
	}
	return NewSource(p.transfer.ID, p.cp, src, p.logger, p.registry)
}

func (p *Provider) Activate(context.Context, *model.TransferOperation, abstract.TableMap, providers.ActivateCallbacks) error {
	if p.transfer.SrcType() == ProviderType && !p.transfer.IncrementOnly() {
		return xerrors.New("Only allowed mode for kinesis source is replication")
	}
	return nil
}

func New(lgr log.Logger, registry metrics.Registry, cp cpclient.Coordinator, transfer *model.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
