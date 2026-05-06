package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/errgroup"
)

const (
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

	oraclePartSize = 1 * 1024 * 1024 * 1024
)

type oracleParallelTableSource struct {
	sqlxDB           *sqlx.DB
	splitTransaction *sqlx.Tx
	config           *provider_oracle.OracleSource
	position         *oracle_common.LogPosition
	table            *oracle_schema.Table
	sourceStats      *stats.SourceStats
	logger           log.Logger
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
	load    *loader
	pusher  abstract.Pusher
	partRow TablePartRow
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
	//nolint:exhaustivestruct
	return &oracleParallelTableSource{
		sqlxDB:           sqlxDB,
		splitTransaction: splitTransaction,
		config:           config,
		position:         position,
		table:            table,
		sourceStats:      sourceStats,
		logger:           logger,
	}, nil
}

func (s *oracleParallelTableSource) Load(ctx context.Context, pusher abstract.Pusher) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var partRows []TablePartRow
	if err := s.splitTransaction.Select(&partRows, splitByRowIDSQLTemplate,
		oraclePartSize, s.table.OracleSchema().OracleName(), s.table.OracleName()); err != nil {
		return xerrors.Errorf("failed to execute a query to split table into parts: %w", err)
	}

	g, ctx := errgroup.WithContext(runCtx)
	g.SetLimit(s.config.ParallelTableLoadDegreeOfParallelism)
	for i := 0; i < len(partRows); i++ {
		part := partLoadState{
			load:    newLoader(s.sqlxDB, s.config, s.position, s.table, s.logger, s.sourceStats),
			pusher:  pusher,
			partRow: partRows[i]}
		g.Go(func() error {
			if err := s.loadPart(ctx, part); err != nil {
				return xerrors.Errorf("failed to load part loading routine: %w", err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return xerrors.Errorf("oracleParallelTableSource.Load err: %w", err)
	}

	return nil
}

func (s *oracleParallelTableSource) loadPart(ctx context.Context, state partLoadState) error {
	columnsSQL, err := getSelectColumns(s.table)
	if err != nil {
		return xerrors.Errorf("Can't create select columns SQL for table '%v': %w", s.table.OracleSQLName(), err)
	}

	var sqlQuery string
	if s.config.IsNonConsistentSnapshot {
		sqlQuery = fmt.Sprintf("select %v from %v where %v",
			columnsSQL, s.table.OracleSQLName(), state.partRow.WhereClause)
	} else {
		sqlQuery = fmt.Sprintf("select %v from %v as of scn %v where %v",
			columnsSQL, s.table.OracleSQLName(), state.partRow.CurrentSCN, state.partRow.WhereClause)
	}

	return state.load.LoadSnapshot(ctx, state.pusher, sqlQuery)
}
