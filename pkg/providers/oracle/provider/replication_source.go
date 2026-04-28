package provider

import (
	"github.com/jmoiron/sqlx"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	logminer "github.com/transferia/transferia/pkg/providers/oracle/replication/log_miner"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.Source = (*source)(nil)

type source struct {
	inner *logminer.LogMinerSource
	db    *sqlx.DB
}

func newsource(
	logger log.Logger,
	registry core_metrics.Registry,
	cp coordinator.Coordinator,
	cfg *provider_oracle.OracleSource,
	transferID string,
) (*source, error) {
	sqlxDB, err := oracle_common.CreateConnection(cfg)
	if err != nil {
		return nil, xerrors.Errorf("Can't create connection: %w", err)
	}

	schemaRepo, err := oracle_schema.NewDatabase(sqlxDB, cfg, logger)
	if err != nil {
		return nil, xerrors.Errorf("Can't create schema repository: %w", err)
	}
	if err := schemaRepo.LoadMetadata(); err != nil {
		return nil, xerrors.Errorf("Can't load metadata: %w", err)
	}
	if err := schemaRepo.LoadTablesFromConfig(); err != nil {
		return nil, xerrors.Errorf("Can't load tables from config: %w", err)
	}

	tracker, err := createLogTracker(sqlxDB, cp, cfg, transferID)
	if err != nil {
		return nil, xerrors.Errorf("Can't create log tracker: %w", err)
	}
	if tracker != nil {
		if err := tracker.Init(); err != nil {
			return nil, xerrors.Errorf("Can't init tracker: %w", err)
		}
	}

	inner := logminer.NewLogMinerSource(sqlxDB, cfg, schemaRepo, tracker, logger, stats.NewSourceStats(registry))

	return &source{inner: inner, db: sqlxDB}, nil
}

func (s *source) Run(sink abstract.AsyncSink) error {
	return s.inner.Run(sink)
}

func (s *source) Stop() {
	s.inner.Stop()
	if s.db != nil {
		_ = s.db.Close()
		s.db = nil
	}
}
