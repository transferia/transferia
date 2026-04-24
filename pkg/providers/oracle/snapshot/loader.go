package snapshot

import (
	"context"
	"database/sql"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/middlewares/asynchronizer"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

// loader is the basic provider of snapshot table load for Oracle source
type loader struct {
	sqlxDB   *sqlx.DB
	config   *provider_oracle.OracleSource
	table    *oracle_schema.Table
	position *oracle_common.LogPosition
	metrics  *stats.SourceStats

	current uint64

	logger log.Logger
}

func newLoader(sqlxDB *sqlx.DB, config *provider_oracle.OracleSource, position *oracle_common.LogPosition, table *oracle_schema.Table, logger log.Logger, metrics *stats.SourceStats) *loader {
	return &loader{
		sqlxDB:   sqlxDB,
		config:   config,
		table:    table,
		position: position,
		metrics:  metrics,

		current: 0,

		logger: logger,
	}
}

// rowsInBatch determines the size of the batch sent into bufferer. It should be small enough to ensure the buffer is not too overflowed
const rowsInBatch int = 512

func estimateRowSize(values []interface{}) uint64 {
	var sum uint64
	for _, v := range values {
		switch vv := v.(type) {
		case *string:
			if !oracle_common.IsNullString(vv) {
				sum += uint64(len(*vv))
			}
		case *[]byte:
			if vv != nil && *vv != nil {
				sum += uint64(len(*vv))
			}
		case *sql.NullString:
			if vv != nil && vv.Valid {
				sum += uint64(len(vv.String))
			}
		case *sql.NullInt64:
			if vv != nil && vv.Valid {
				sum += 8
			}
		case *sql.NullFloat64:
			if vv != nil && vv.Valid {
				sum += 8
			}
		case *sql.NullTime:
			if vv != nil && vv.Valid {
				sum += 16
			}
		}
	}
	return sum
}

// LoadSnapshot implements snapshot load to the given target
func (l *loader) LoadSnapshot(ctx context.Context, syncTarget asynchronizer.Asynchronizer, sql string) error {
	rawValues, err := createRawValues(l.table)
	if err != nil {
		return xerrors.Errorf("Can't create raw values for table '%v': %w", l.table.OracleSQLName(), err)
	}

	batch := []abstract2.Event{}
	var batchBytes uint64
	batchTime := time.Now()
	queryErr := oracle_common.PDBQueryGlobal(
		l.config,
		l.sqlxDB,
		ctx,
		func(ctx context.Context, connection *sqlx.Conn) error {
			rows, err := connection.QueryContext(ctx, sql)
			if err != nil {
				return xerrors.Errorf("Can't select table '%v': %w", l.table.OracleSQLName(), err)
			}
			defer rows.Close()

			for rows.Next() {
				if err := rows.Scan(rawValues...); err != nil {
					return xerrors.Errorf("Can't scan values for table '%v': %w", l.table.OracleSQLName(), err)
				}

				if l.metrics != nil {
					batchBytes += estimateRowSize(rawValues)
				}

				event, err := createInsertEvent(l.table, rawValues, l.position)
				if err != nil {
					return xerrors.Errorf("Can't parse raw values for table '%v': %w", l.table.OracleSQLName(), err)
				}
				batch = append(batch, event)

				if len(batch) >= rowsInBatch {
					if err := l.pushBatch(batch, batchBytes, &batchTime, syncTarget); err != nil {
						return xerrors.Errorf("failed to push to target: %w", err)
					}
					batch = []abstract2.Event{}
					batchBytes = 0
				}
			}
			if rows.Err() != nil {
				return xerrors.Errorf("Can't read row from DB: %w", rows.Err())
			}

			return nil
		},
	)
	if queryErr != nil {
		return xerrors.Errorf("failed while executing query '%s' in Oracle: %w", sql, queryErr)
	}

	if len(batch) > 0 {
		if err := l.pushBatch(batch, batchBytes, &batchTime, syncTarget); err != nil {
			return xerrors.Errorf("failed to push to target: %w", err)
		}
		batch = []abstract2.Event{}
	}

	return nil
}

func (l *loader) pushBatch(batch []abstract2.Event, batchBytes uint64, batchTime *time.Time, syncTarget asynchronizer.Asynchronizer) error {
	if err := syncTarget.Push(abstract2.NewEventBatch(batch)); err != nil {
		return xerrors.Errorf("failed to push a batch of %d events: %w", len(batch), err)
	}
	if l.metrics != nil {
		l.metrics.Size.Add(int64(batchBytes))
		l.metrics.ChangeItems.Add(int64(len(batch)))
	}
	atomic.AddUint64(&l.current, uint64(len(batch)))
	*batchTime = time.Now()
	return nil
}

func (l *loader) Current() uint64 {
	return atomic.LoadUint64(&l.current)
}
