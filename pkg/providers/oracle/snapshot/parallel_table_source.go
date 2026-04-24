package snapshot

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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

const (
	// Запрос для разбиения таблички на части по ROWID
	// Оригинал - https://bb.yandexcloud.net/projects/BI/repos/yt-scripts/browse/scripts/etl_oracle/many_tables_to_yt_by_extents.py#231
	// Если вы не понимаете, что тут написано - это нормально, объяснение можно спросить у @darkwingduck или @xifos
	// Запрос содержит 3 параметра (в порядке объявления): ':part_size', ':owner', ':table_name'
	splitByRowIDSQLTemplate = `
WITH clauses AS
         (SELECT NVL(
                             CASE WHEN rn > 1 THEN 'ROWID >= CHARTOROWID(''' || row_id || ''')' END
                             || CASE WHEN rn > 1 AND rn < cnt THEN ' AND ' END
                             || CASE WHEN rn < cnt THEN 'ROWID < CHARTOROWID(''' || LEAD(row_id) OVER (ORDER BY rn) || ''')' END,
                             '1=1') where_clause,
                 total_cnt,
                 rn,
                 sum_bytes
          FROM (SELECT ex.*, ROW_NUMBER() OVER (ORDER BY CHARTOROWID(row_id)) rn, COUNT(1) OVER (PARTITION BY 1) cnt
                FROM (SELECT ex.*,
                             ROW_NUMBER() OVER (PARTITION BY end_part ORDER BY row_id) best_fit_rn,
                             SUM(bytes) OVER (PARTITION BY end_part)                   sum_bytes
                      FROM (SELECT ex.*,
                                   COUNT(1) OVER (PARTITION BY 1) total_cnt,
                                   CEIL(SUM(bytes) OVER (ORDER BY CHARTOROWID(row_id) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / (:part_size)) end_part
                            FROM (SELECT ex.*,
                                         DBMS_ROWID.rowid_create(1,
                                                                 data_object_id,
                                                                 relative_fno,
                                                                 block_id,
                                                                 0) row_id
                                  FROM dba_extents ex
                                           JOIN dba_objects obj
                                                ON obj.owner = ex.owner
                                                    AND obj.object_name = ex.segment_name
                                                    AND obj.object_type = ex.segment_type
                                                    AND DECODE(obj.subobject_name, ex.partition_name, 1, 0) = 1
                                  WHERE ex.owner = :owner AND ex.segment_name = :table_name) ex) ex) ex
                WHERE best_fit_rn = 1) ex),
     clauses_any
         AS
         (SELECT where_clause, total_cnt, rn, sum_bytes
          FROM clauses
          UNION ALL
          SELECT '1=1' where_clause, 0 total_cnt, 1 rn, 0 sum_bytes
          FROM DUAL
          WHERE (SELECT COUNT(1) FROM clauses) = 0)
SELECT where_clause,
       total_cnt                            extent_count,
       MAX(rn) OVER (PARTITION BY 1)        oracle_result_partition_count,
       (SELECT current_scn FROM v$database) current_scn,
       sys_extract_utc(systimestamp)        current_timestamp,
       MAX(sum_bytes) OVER (PARTITION BY 1) max_bytes,
       MIN(sum_bytes) OVER (PARTITION BY 1) min_bytes
FROM clauses_any
ORDER BY rn
`

	oraclePartSize = 1 * 1024 * 1024 * 1024 // 1gb
)

type oracleParallelTableSource struct {
	sqlxDB           *sqlx.DB
	splitTransaction *sqlx.Tx
	config           *provider_oracle.OracleSource
	position         *oracle_common.LogPosition
	table            *oracle_schema.Table
	sourceStats      *stats.SourceStats

	state oracleParallelTableSourceRunState

	partStates []partLoadState

	total uint64

	logger log.Logger
}

type oracleParallelTableSourceRunState struct {
	sync.Mutex
	Cancel      context.CancelFunc
	HasFinished bool
}

type TablePartRow struct {
	WhereClause                string    `db:"WHERE_CLAUSE"`
	ExtentCount                int       `db:"EXTENT_COUNT"`
	OracleResultPartitionCount int       `db:"ORACLE_RESULT_PARTITION_COUNT"`
	CurrentSCN                 uint64    `db:"CURRENT_SCN"`
	CurrentTimestamp           time.Time `db:"CURRENT_TIMESTAMP"`
	MaxBytes                   string    `db:"MAX_BYTES"`
	MinBytes                   string    `db:"MIN_BYTES"`
}

type partLoadState struct {
	load       *loader
	ctx        context.Context
	syncTarget asynchronizer.Asynchronizer
	partRow    TablePartRow
}

func NewParallelTableSource(
	sqlxDB *sqlx.DB,
	splitTransaction *sqlx.Tx,
	config *provider_oracle.OracleSource,
	position *oracle_common.LogPosition,
	table *oracle_schema.Table,
	logger log.Logger,
	sourceStats *stats.SourceStats,
) (*oracleParallelTableSource, error) {
	total, err := getRowsCount(logger, config, sqlxDB, table)
	if err != nil {
		return nil, xerrors.Errorf("Can't get rows count for table '%v': %w", table.OracleSQLName(), err)
	}

	return &oracleParallelTableSource{
		sqlxDB:           sqlxDB,
		splitTransaction: splitTransaction,
		config:           config,
		position:         position,
		table:            table,
		sourceStats:      sourceStats,

		state: oracleParallelTableSourceRunState{
			Mutex:       sync.Mutex{},
			Cancel:      nil,
			HasFinished: false,
		},

		partStates: nil,

		total: total,

		logger: logger,
	}, nil
}

func (s *oracleParallelTableSource) Start(ctx context.Context, target abstract2.EventTarget) error {
	s.state.Lock()
	if s.state.Cancel != nil {
		s.state.Unlock()
		return xerrors.Errorf("failed to Start: the parallel source is already running")
	}
	runCtx, cancF := context.WithCancel(ctx)
	s.state.Cancel = cancF
	defer s.Stop()
	s.state.Unlock()

	ddlSyncTarget := asynchronizer.NewEventTargetWrapper(target)
	ddlSyncTargetRollbacks := util.Rollbacks{}
	ddlSyncTargetRollbacks.Add(func() {
		if err := ddlSyncTarget.Close(); err != nil {
			s.logger.Error("Failed to push events into target for a part", log.Error(err))
		}
	})
	defer ddlSyncTargetRollbacks.Do()

	if err := ddlSyncTarget.Push(abstract2.NewEventBatch([]abstract2.Event{events.NewDefaultTableLoadEvent(s.table, events.TableLoadBegin)})); err != nil {
		return xerrors.Errorf("failed to push TableLoadBegin: %w", err)
	}

	// prepare parts and queue them
	var partRows []TablePartRow
	if err := s.splitTransaction.Select(&partRows, splitByRowIDSQLTemplate,
		oraclePartSize, s.table.OracleSchema().OracleName(), s.table.OracleName()); err != nil {
		return xerrors.Errorf("failed to execute a query to split table into parts: %w", err)
	}
	s.state.Lock()
	s.partStates = make([]partLoadState, len(partRows))
	for i := 0; i < len(partRows); i++ {
		s.partStates[i] = partLoadState{
			load:       newLoader(s.sqlxDB, s.config, s.position, s.table, s.logger, s.sourceStats),
			ctx:        runCtx,
			syncTarget: asynchronizer.NewEventTargetWrapper(target),
			partRow:    partRows[i],
		}
	}
	s.state.Unlock()
	partSyncTargetRollbacks := util.Rollbacks{}
	partSyncTargetRollbacks.Add(func() {
		for i := 0; i < len(s.partStates); i++ {
			if err := s.partStates[i].syncTarget.Close(); err != nil {
				s.logger.Error("Failed to push events into target for a part", log.Int("part_index", i), log.Error(err))
			}
		}
	})
	partLoadQueue := make(chan *partLoadState, len(s.partStates))
	for i := 0; i < len(partRows); i++ {
		partLoadQueue <- &s.partStates[i]
	}
	close(partLoadQueue)

	// load parts and wait for routines
	errCh := make(chan error, s.config.ParallelTableLoadDegreeOfParallelism)
	terminateCh := make(chan struct{})
	for i := 0; i < s.config.ParallelTableLoadDegreeOfParallelism; i++ {
		go s.partLoadingRoutine(partLoadQueue, errCh, terminateCh)
	}
	for i := 0; i < s.config.ParallelTableLoadDegreeOfParallelism; i++ {
		if err := <-errCh; err != nil {
			close(terminateCh)
			return xerrors.Errorf("part-loading routine [%d] failed: %w", i, err)
		}
	}

	// close sync targets
	partSyncTargetRollbacks.Cancel()
	var closeErrs []error
	for i := 0; i < len(s.partStates); i++ {
		if err := s.partStates[i].syncTarget.Close(); err != nil {
			closeErrs = append(closeErrs, xerrors.Errorf("failed to push events into target for a part [%d]: %w", i, err))
		}
	}
	if err := errors.Join(closeErrs...); err != nil {
		return xerrors.Errorf("failed to push events for one or more parts: %w", err)
	}

	if err := ddlSyncTarget.Push(abstract2.NewEventBatch([]abstract2.Event{events.NewDefaultTableLoadEvent(s.table, events.TableLoadEnd)})); err != nil {
		return xerrors.Errorf("failed to push TableLoadEnd: %w", err)
	}

	ddlSyncTargetRollbacks.Cancel()
	if err := ddlSyncTarget.Close(); err != nil {
		return xerrors.Errorf("failed to push events into target: %w", err)
	}

	s.state.Lock()
	s.state.HasFinished = true
	s.state.Unlock()

	return nil
}

func (s *oracleParallelTableSource) partLoadingRoutine(inputCh chan *partLoadState, errCh chan error, terminateCh chan struct{}) {
	for state := range inputCh {
		select {
		case <-terminateCh:
			// terminate routine
			errCh <- nil
			return
		default:
			// normal operation
		}
		if err := s.loadPart(state); err != nil {
			errCh <- err
			return
		}
	}
	errCh <- nil
}

func (s *oracleParallelTableSource) loadPart(state *partLoadState) error {
	columnsSQL, err := getSelectColumns(s.table)
	if err != nil {
		return xerrors.Errorf("Can't create select columns SQL for table '%v': %w", s.table.OracleSQLName(), err)
	}

	var sql string
	if s.config.IsNonConsistentSnapshot {
		sql = fmt.Sprintf("select %v from %v where %v",
			columnsSQL, s.table.OracleSQLName(), state.partRow.WhereClause)
	} else {
		sql = fmt.Sprintf("select %v from %v as of scn %v where %v",
			columnsSQL, s.table.OracleSQLName(), state.partRow.CurrentSCN, state.partRow.WhereClause)
	}

	return state.load.LoadSnapshot(state.ctx, state.syncTarget, sql)
}

func (s *oracleParallelTableSource) Running() bool {
	s.state.Lock()
	defer s.state.Unlock()

	return s.state.Cancel != nil
}

func (s *oracleParallelTableSource) Progress() (abstract2.EventSourceProgress, error) {
	s.state.Lock()
	defer s.state.Unlock()

	if s.state.Cancel == nil || len(s.partStates) == 0 {
		return abstract2.NewDefaultEventSourceProgress(false, 0, s.total), nil
	}

	current := uint64(0)
	for i := 0; i < len(s.partStates); i++ {
		current += s.partStates[i].load.Current()
	}

	return abstract2.NewDefaultEventSourceProgress(s.state.HasFinished, current, s.total), nil
}

func (s *oracleParallelTableSource) Stop() error {
	s.state.Lock()
	defer s.state.Unlock()
	if s.state.Cancel != nil {
		s.state.Cancel()
		s.state.Cancel = nil
	}
	return nil
}
