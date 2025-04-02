package ydb

import (
	"context"

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
	gobwrapper.RegisterName("*server.YdbDestination", new(YdbDestination))
	gobwrapper.RegisterName("*server.YdbSource", new(YdbSource))
	model.RegisterDestination(ProviderType, func() model.Destination {
		return new(YdbDestination)
	})
	model.RegisterSource(ProviderType, func() model.Source {
		return new(YdbSource)
	})

	abstract.RegisterProviderName(ProviderType, "YDB")
	providers.Register(ProviderType, New)
}

const ProviderType = abstract.ProviderType("ydb")

// To verify providers contract implementation
var (
	_ providers.Snapshot    = (*Provider)(nil)
	_ providers.Replication = (*Provider)(nil)
	_ providers.Sinker      = (*Provider)(nil)

	_ providers.Activator   = (*Provider)(nil)
	_ providers.Deactivator = (*Provider)(nil)
	_ providers.Cleanuper   = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       coordinator.Coordinator
	transfer *model.Transfer
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*YdbSource)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	p.fillIncludedTables(src)
	return NewStorage(src.ToStorageParams(), p.registry)
}

func (p *Provider) fillIncludedTables(src *YdbSource) {
	include := p.transfer.DataObjects.GetIncludeObjects()
	if len(include) == 0 {
		return
	}

	result := make([]string, 0)
	for _, table := range include {
		tid := abstract.TableID{Namespace: "", Name: table}
		if src.Include(tid) {
			result = append(result, table)
		}
	}
	src.Tables = result
}

func (p *Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*YdbSource)
	if !ok {
		return nil, xerrors.Errorf("Unknown source type: %T", p.transfer.Src)
	}
	p.fillIncludedTables(src)

	err := CreateChangeFeedIfNotExists(src, p.transfer.ID)
	if err != nil {
		return nil, xerrors.Errorf("unable to upsert changeFeed, err: %w", err)
	}
	return NewSource(p.transfer.ID, src, p.logger, p.registry)
}

func (p *Provider) Activate(ctx context.Context, task *model.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	src, ok := p.transfer.Src.(*YdbSource)
	if !ok {
		return xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}
	p.fillIncludedTables(src)

	if !p.transfer.SnapshotOnly() {
		if len(src.Tables) == 0 {
			return xerrors.Errorf("unable to replicate all tables in the database")
		}
		err := DropChangeFeed(src, p.transfer.ID)
		if err != nil {
			return xerrors.Errorf("unable to drop changeFeed, err: %w", err)
		}
		err = CreateChangeFeed(src, p.transfer.ID)
		if err != nil {
			return xerrors.Errorf("unable to create changeFeed, err: %w", err)
		}
	}
	if !p.transfer.IncrementOnly() {
		if err := callbacks.Cleanup(ConvertTableMapToYDBRelPath(src.ToStorageParams(), tables)); err != nil {
			return xerrors.Errorf("Sinker cleanup failed: %w", err)
		}
		if err := callbacks.CheckIncludes(tables); err != nil {
			return xerrors.Errorf("Failed in accordance with configuration: %w", err)
		}
		if err := callbacks.Upload(tables); err != nil {
			return xerrors.Errorf("Snapshot loading failed: %w", err)
		}
	}
	return nil
}

func (p *Provider) Deactivate(ctx context.Context, task *model.TransferOperation) error {
	src, ok := p.transfer.Src.(*YdbSource)
	if !ok {
		return xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}
	p.fillIncludedTables(src)

	if !p.transfer.SnapshotOnly() {
		err := DropChangeFeed(src, p.transfer.ID)
		if err != nil {
			return xerrors.Errorf("drop changefeed error occurred: %w", err)
		}
	}
	return nil
}

func (p *Provider) Cleanup(ctx context.Context, task *model.TransferOperation) error {
	src, ok := p.transfer.Src.(*YdbSource)
	if !ok {
		return xerrors.Errorf("unexpected src type: %T", p.transfer.Src)
	}
	p.fillIncludedTables(src)

	return DropChangeFeed(src, p.transfer.ID)
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func (p *Provider) Sink(middlewares.Config) (abstract.Sinker, error) {
	dst, ok := p.transfer.Dst.(*YdbDestination)
	if !ok {
		return nil, xerrors.Errorf("unexpected target type: %T", p.transfer.Dst)
	}
	return NewSinker(p.logger, dst, p.registry)
}

func New(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, transfer *model.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
