//go:build !disable_airbyte_provider

package airbyte

import (
	"context"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	"go.ytsaurus.tech/library/go/core/log"
)

func init() {
	providers.Register(ProviderType, New)
}

// To verify providers contract implementation
var (
	_ providers.Activator   = (*Provider)(nil)
	_ providers.Snapshot    = (*Provider)(nil)
	_ providers.Replication = (*Provider)(nil)
	_ providers.Tester      = (*Provider)(nil)
)

type Provider struct {
	logger   log.Logger
	registry metrics.Registry
	cp       cpclient.Coordinator
	transfer *model.Transfer
}

func (p *Provider) Source() (abstract.Source, error) {
	src, ok := p.transfer.Src.(*AirbyteSource)
	if !ok {
		return nil, xerrors.Errorf("unknown src type: %T", p.transfer.Src)
	}
	return NewSource(p.logger, p.registry, p.cp, src, p.transfer), nil
}

func (p *Provider) Activate(ctx context.Context, task *model.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	if !p.transfer.IncrementOnly() {
		toCleanup := abstract.TableMap{}
		state, err := p.cp.GetTransferState(p.transfer.ID)
		if err != nil {
			return xerrors.Errorf("unable to extract transfer state: %w", err)
		}
		for tid := range tables {
			airbyteState, ok := state[StateKey(tid)]
			if ok && airbyteState != nil && airbyteState.Generic != nil {
				p.logger.Info("table have state, skip from cleanup", log.Any("table", tid))
			} else {
				toCleanup[tid] = *new(abstract.TableInfo)
			}
		}
		if err := callbacks.Cleanup(toCleanup); err != nil {
			return xerrors.Errorf("sinker cleanup failed: %w", err)
		}
		if err := callbacks.CheckIncludes(tables); err != nil {
			return xerrors.Errorf("failed in accordance with configuration: %w", err)
		}
		if err := callbacks.Upload(tables); err != nil {
			return xerrors.Errorf("snapshot loading failed: %w", err)
		}
	}
	return nil
}

func (p *Provider) Storage() (abstract.Storage, error) {
	src, ok := p.transfer.Src.(*AirbyteSource)
	if !ok {
		return nil, xerrors.Errorf("unknown src type: %T", p.transfer.Src)
	}
	return NewStorage(p.logger, p.registry, p.cp, src, p.transfer)
}

const (
	DiscoveryCheck = abstract.CheckType("discover")
)

func (p *Provider) TestChecks() []abstract.CheckType {
	return []abstract.CheckType{DiscoveryCheck}
}

func (p *Provider) Test(ctx context.Context) *abstract.TestResult {
	src, ok := p.transfer.Src.(*AirbyteSource)
	if !ok {
		return nil
	}
	tr := abstract.NewTestResult(p.TestChecks()...)
	storage, _ := NewStorage(p.logger, p.registry, p.cp, src, p.transfer)
	tables, err := storage.TableList(nil)
	if err != nil {
		return tr.NotOk(DiscoveryCheck, err)
	}
	tr.Schema = tables
	return tr.Ok(DiscoveryCheck)
}

func (p *Provider) Type() abstract.ProviderType {
	return ProviderType
}

func New(lgr log.Logger, registry metrics.Registry, cp cpclient.Coordinator, transfer *model.Transfer) providers.Provider {
	return &Provider{
		logger:   lgr,
		registry: registry,
		cp:       cp,
		transfer: transfer,
	}
}
