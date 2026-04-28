package provider

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
)

var _ providers.Activator = (*Provider)(nil)

func resetOracleTracker(cp coordinator.Coordinator, transfer *model.Transfer) error {
	src, ok := transfer.Src.(*provider_oracle.OracleSource)
	if !ok {
		return nil
	}
	if src.TrackerType == provider_oracle.OracleNoLogTracker {
		return nil
	}

	cfg := *src
	if transfer.SnapshotOnly() && cfg.TrackerType == provider_oracle.OracleEmbeddedLogTracker {
		cfg.TrackerType = provider_oracle.OracleInMemoryLogTracker
	}

	sqlxDB, err := oracle_common.CreateConnection(&cfg)
	if err != nil {
		return xerrors.Errorf("Can't create connection for tracker reset: %w", err)
	}
	defer func() { _ = sqlxDB.Close() }()

	tracker, err := createLogTracker(sqlxDB, cp, &cfg, transfer.ID)
	if err != nil {
		return xerrors.Errorf("create log tracker: %w", err)
	}
	if tracker == nil {
		return nil
	}
	if err := tracker.Init(); err != nil {
		return xerrors.Errorf("Can't init tracker: %w", err)
	}
	pos, err := getCurrentLogPosition(sqlxDB, transfer.Type)
	if err != nil {
		return xerrors.Errorf("get current log position: %w", err)
	}
	if err := tracker.WritePosition(pos); err != nil {
		return xerrors.Errorf("write tracker position: %w", err)
	}
	return nil
}

func (p *Provider) Activate(ctx context.Context, task *model.TransferOperation, tables abstract.TableMap, callbacks providers.ActivateCallbacks) error {
	if err := resetOracleTracker(p.cp, p.transfer); err != nil {
		return xerrors.Errorf("oracle tracker reset: %w", err)
	}
	if !p.transfer.IncrementOnly() {
		if err := callbacks.Cleanup(tables); err != nil {
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
