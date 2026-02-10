package greenplum

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	destinationFactory := func() model.LoggableDestination {
		return new(GpDestination)
	}
	model.RegisterDestination(ProviderType, destinationFactory)
	model.RegisterSource(ProviderType, func() model.LoggableSource {
		return new(GpSource)
	})

	abstract.RegisterProviderName(ProviderType, "Greenplum")
	providers.Register(ProviderType, New)

	gobwrapper.RegisterName("*server.GpSource", new(GpSource))
	gobwrapper.RegisterName("*server.GpDestination", new(GpDestination))

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
	if err := dst.Connection.ResolveCredsFromConnectionID(); err != nil {
		return nil, xerrors.Errorf("failed to resolve creds from connection ID: %w", err)
	}
	if gpfdistParams := p.asGpfdist(); gpfdistParams != nil {
		sink, err := NewGpfdistSink(dst, p.registry, p.logger, p.transfer.ID, *gpfdistParams)
		if err == nil {
			p.logger.Warn("Using experimental gfpdist sink")
			return sink, nil
		}
		p.logger.Warn("Cannot use experimental gfpdist sink", log.Error(err))
	}
	return NewSink(p.transfer, p.registry, p.logger, config)
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*GpSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}
	if err := src.Connection.ResolveCredsFromConnectionID(); err != nil {
		return nil, xerrors.Errorf("failed to resolve creds from connection ID: %w", err)
	}
	if gpfdistParams := p.asGpfdist(); gpfdistParams != nil {
		p.logger.Warn("Using experimental gfpdist storage")
		return NewGpfdistStorage(src, p.registry, *gpfdistParams), nil
	}
	return NewStorage(src, p.registry), nil
}

// asGpfdist checks that gpfdist could be used and returns gpfdist params or nil.
// For now, gpfdist is used only for GP->GP transfers if GpSource.AdvancedProps.DisableGpfdist is false.
func (p *Provider) asGpfdist() *gpfdistbin.GpfdistParams {
	src, isGpSrc := p.transfer.Src.(*GpSource)
	_, isGpDst := p.transfer.Dst.(*GpDestination)
	if !isGpSrc || !isGpDst || src.AdvancedProps.DisableGpfdist {
		return nil
	}
	gpfdistParams := gpfdistbin.NewGpfdistParams(
		src.AdvancedProps.GpfdistBinPath,
		src.AdvancedProps.ServiceSchema,
		p.transfer.ParallelismParams().ProcessCount,
	)
	return gpfdistParams
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func New(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer, _ *model.TransferOperation) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
