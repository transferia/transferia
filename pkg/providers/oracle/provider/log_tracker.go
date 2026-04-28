package provider

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	"github.com/transferia/transferia/pkg/providers/oracle/logtracker"
)

func createLogTracker(
	sqlxDB *sqlx.DB,
	controlPlaneClient coordinator.Coordinator,
	config *provider_oracle.OracleSource,
	transferID string,
) (logtracker.LogTracker, error) {
	switch config.TrackerType {
	case provider_oracle.OracleNoLogTracker:
		return nil, nil
	case provider_oracle.OracleInMemoryLogTracker:
		inMemoryLogTracker, err := logtracker.NewInMemoryLogTracker(transferID)
		if err != nil {
			return nil, xerrors.Errorf("Can't create in memory log tracker: %w", err)
		}
		return inMemoryLogTracker, nil
	case provider_oracle.OracleEmbeddedLogTracker:
		embeddedLogTracker, err := logtracker.NewEmbeddedLogTracker(sqlxDB, config, transferID)
		if err != nil {
			return nil, xerrors.Errorf("Can't create embedded log tracker: %w", err)
		}
		return embeddedLogTracker, nil
	case provider_oracle.OracleInternalLogTracker:
		internalLogTracker, err := logtracker.NewInternalLogTracker(controlPlaneClient, transferID)
		if err != nil {
			return nil, xerrors.Errorf("Can't create internal log tracker: %w", err)
		}
		return internalLogTracker, nil
	default:
		return nil, xerrors.Errorf("Unknown tracker type '%v'", config.TrackerType)
	}
}

func getCurrentLogPosition(sqlxDB *sqlx.DB, transferType abstract.TransferType) (*oracle_common.LogPosition, error) {
	sqlQuery := "SELECT CURRENT_SCN, sys_extract_utc(systimestamp) as CURRENT_TIMESTAMP FROM v$database"
	var sqlResult struct {
		SCN              uint64    `db:"CURRENT_SCN"`
		CurrentTimestamp time.Time `db:"CURRENT_TIMESTAMP"`
	}
	if err := sqlxDB.Get(&sqlResult, sqlQuery); err != nil {
		return nil, xerrors.Errorf("Can't select current SCN from DB: %w", err)
	}

	var positionType oracle_common.PositionType
	switch transferType {
	case abstract.TransferTypeSnapshotOnly, abstract.TransferTypeSnapshotAndIncrement:
		positionType = oracle_common.PositionSnapshotStarted
	case abstract.TransferTypeIncrementOnly:
		positionType = oracle_common.PositionReplication
	default:
		return nil, xerrors.Errorf("Transfer type '%v' is not supported", transferType)
	}

	return oracle_common.NewLogPosition(sqlResult.SCN, nil, nil, positionType, sqlResult.CurrentTimestamp)
}
