package snapshot

import (
	"context"
	"fmt"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/abstract2/events"
	"github.com/transferia/transferia/pkg/middlewares/asynchronizer"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type oracleTableSource struct {
	sqlxDB   *sqlx.DB
	config   *provider_oracle.OracleSource
	position *oracle_common.LogPosition
	table    *oracle_schema.Table

	state oracleParallelTableSourceRunState

	load *loader

	total uint64

	logger log.Logger
}

type oracleTableSourceRunState struct {
	sync.Mutex
	cancel      context.CancelFunc
	hasFinished bool
}

func NewTableSource(
	sqlxDB *sqlx.DB,
	config *provider_oracle.OracleSource,
	position *oracle_common.LogPosition,
	table *oracle_schema.Table,
	logger log.Logger,
	sourceStats *stats.SourceStats,
) (*oracleTableSource, error) {
	if position != nil {
		if !position.OnlySCN() {
			return nil, xerrors.Errorf("position error: Can start from SCN only")
		}
	}

	count, err := getRowsCount(logger, config, sqlxDB, table)
	if err != nil {
		return nil, xerrors.Errorf("failed to get rows count for table '%s': %w", table.OracleSQLName(), err)
	}

	return &oracleTableSource{
		sqlxDB:   sqlxDB,
		config:   config,
		position: position,
		table:    table,

		state: oracleParallelTableSourceRunState{
			Mutex:       sync.Mutex{},
			Cancel:      nil,
			HasFinished: false,
		},

		load: newLoader(sqlxDB, config, position, table, logger, sourceStats),

		total: count,

		logger: logger,
	}, nil
}

func (s *oracleTableSource) Running() bool {
	s.state.Lock()
	defer s.state.Unlock()
	return s.state.Cancel != nil
}

func (s *oracleTableSource) Progress() (abstract2.EventSourceProgress, error) {
	s.state.Lock()
	defer s.state.Unlock()
	return abstract2.NewDefaultEventSourceProgress(s.state.HasFinished, s.load.Current(), s.total), nil
}

func (s *oracleTableSource) Start(ctx context.Context, target abstract2.EventTarget) error {
	s.state.Lock()
	if s.state.Cancel != nil {
		s.state.Unlock()
		return xerrors.Errorf("failed to Start: the source is already running")
	}
	runCtx, cancF := context.WithCancel(ctx)
	s.state.Cancel = cancF
	defer s.Stop()
	s.state.Unlock()

	syncTarget := asynchronizer.NewEventTargetWrapper(target)
	rollbacks := util.Rollbacks{}
	rollbacks.Add(func() {
		if err := syncTarget.Close(); err != nil {
			s.logger.Error("Failed to push events (asynchronously)", log.Error(err))
		}
	})
	defer rollbacks.Do()

	if err := syncTarget.Push(abstract2.NewEventBatch([]abstract2.Event{events.NewDefaultTableLoadEvent(s.table, events.TableLoadBegin)})); err != nil {
		return xerrors.Errorf("failed to push TableLoadBegin: %w", err)
	}

	columnsSQL, err := getSelectColumns(s.table)
	if err != nil {
		return xerrors.Errorf("Can't create select columns SQL for table '%v': %w", s.table.OracleSQLName(), err)
	}
	var sql string
	if s.config.IsNonConsistentSnapshot || s.position == nil {
		sql = fmt.Sprintf("select %v from %v", columnsSQL, s.table.OracleSQLName())
	} else {
		sql = fmt.Sprintf("select %v from %v as of scn %v", columnsSQL, s.table.OracleSQLName(), s.position.SCN())
	}
	if err := s.load.LoadSnapshot(runCtx, syncTarget, sql); err != nil {
		return xerrors.Errorf("failed while loading snapshot: %w", err)
	}

	if err := syncTarget.Push(abstract2.NewEventBatch([]abstract2.Event{events.NewDefaultTableLoadEvent(s.table, events.TableLoadEnd)})); err != nil {
		return xerrors.Errorf("failed to push TableLoadEnd: %w", err)
	}

	rollbacks.Cancel()
	if err := syncTarget.Close(); err != nil {
		return xerrors.Errorf("failed to push events (asynchronously): %w", err)
	}

	s.state.Lock()
	s.state.HasFinished = true
	s.state.Unlock()

	return nil
}

func (s *oracleTableSource) Stop() error {
	s.state.Lock()
	defer s.state.Unlock()
	if s.state.Cancel != nil {
		s.state.Cancel()
		s.state.Cancel = nil
	}
	return nil
}
