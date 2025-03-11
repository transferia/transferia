package greenplum

import (
	"context"
	"encoding/gob"

	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/abstract/typesystem"
	"github.com/transferria/transferria/pkg/middlewares"
	"github.com/transferria/transferria/pkg/providers"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	destinationFactory := func() model.Destination {
		return new(GpDestination)
	}
	model.RegisterDestination(ProviderType, destinationFactory)
	model.RegisterSource(ProviderType, func() model.Source {
		return new(GpSource)
	})

	abstract.RegisterProviderName(ProviderType, "Greenplum")
	providers.Register(ProviderType, New)

	gob.RegisterName("*server.GpSource", new(GpSource))
	gob.RegisterName("*server.GpDestination", new(GpDestination))

	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:       2,
			Picker:   typesystem.ProviderType(ProviderType),
			Function: postgres.FallbackNotNullAsNull,
		}
	})
	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:       3,
			Picker:   typesystem.ProviderType(ProviderType),
			Function: postgres.FallbackTimestampToUTC,
		}
	})
	typesystem.AddFallbackSourceFactory(func() typesystem.Fallback {
		return typesystem.Fallback{
			To:       5,
			Picker:   typesystem.ProviderType(ProviderType),
			Function: postgres.FallbackBitAsBytes,
		}
	})
}

const (
	ProviderType = abstract.ProviderType("gp")
)

// To verify providers contract implementation
var (
	_ providers.Snapshot = (*Provider)(nil)
	_ providers.Sinker   = (*Provider)(nil)

	_ providers.Activator = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
}

func (p *Provider) Activate(ctx context.Context, task *model.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	if !p.transfer.SnapshotOnly() || p.transfer.IncrementOnly() {
		return abstract.NewFatalError(xerrors.Errorf("only snapshot mode is allowed for the Greenplum source"))
	}
	if err := callbacks.Cleanup(tables); err != nil {
		return xerrors.Errorf("failed to cleanup sink: %w", err)
	}
	if err := callbacks.CheckIncludes(tables); err != nil {
		return xerrors.Errorf("failed in accordance with configuration: %w", err)
	}
	if err := callbacks.Upload(tables); err != nil {
		return xerrors.Errorf("transfer (snapshot) failed: %w", err)
	}
	return nil
}

func (p *Provider) Sink(config middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*GpDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected dst type: %T", p.transfer.Dst)
	}
	isGpfdist, err := p.isGpfdist()
	if err != nil {
		return nil, xerrors.Errorf("unable to use gpfdist: %w", err)
	}
	if isGpfdist {
		sink, err := NewGpfdistSink(dst, p.registry)
		if err != nil {
			return nil, xerrors.Errorf("unable to create gpfidst sink: %w", err)
		}
		return sink, nil
	}
	return NewSink(p.transfer, p.registry, p.logger, config)
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*GpSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}
	isGpfdist, err := p.isGpfdist()
	if err != nil {
		return nil, xerrors.Errorf("unable to use gpfdist: %w", err)
	}
	if isGpfdist {
		return NewGpfdistStorage(src, p.registry)
	}
	return NewStorage(src, p.registry), nil
}

func (p *Provider) isGpfdist() (bool, error) {
	var isSrcGpfdist, isDstGpfdist bool
	if src, ok := p.transfer.Src.(*GpSource); ok {
		isSrcGpfdist = src.GpfdistParams.IsEnabled
	}
	if dst, ok := p.transfer.Dst.(*GpDestination); ok {
		isDstGpfdist = dst.GpfdistParams.IsEnabled
	}
	if isSrcGpfdist != isDstGpfdist { // TODO: TM-8222: Support gpfdist for hetero transfers.
		return false, xerrors.New("Gpfdist is supported only for gp->gp and should be enabled on both endpoints")
	}
	return isSrcGpfdist, nil
}

func (p *Provider) Type() abstract.ProviderType {
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
