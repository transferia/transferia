package snapshot

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
	oracle_schema "github.com/transferia/transferia/pkg/providers/oracle/schema"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

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
		current:  0,
		logger:   logger,
	}
}

const rowsInBatch int = 512

// batchBytesLimit caps the in-flight memory of a single snapshot batch. The previous
// DBMS_LOB.SUBSTR(blob, 32767, 1) wrap acted as an implicit ~32 KB/row cap; now that
// BLOBs are read via the native godror LOB path, this explicit byte-based flush keeps
// peak memory bounded regardless of column size. The downstream bufferer middleware
// only protects the segment after pusher(...), not this loader's local accumulator.
const batchBytesLimit uint64 = 16 * 1024 * 1024

// ociFetchIdleTimeout caps the wall time between two consecutive rows.Next() calls
// during a snapshot fetch. If the driver stops returning rows within this window
// (network hang, broken cursor, locked block), the fetch context is cancelled,
// LoadSnapshot returns the cancellation error, and the upper retry loop in
// load_snapshot.go retries the part up to 3 times. Hardcoded for POC; lift to
// OracleSource as a configurable parameter once value tuning is needed.
const ociFetchIdleTimeout = 10 * time.Minute

func estimateRowSize(values []interface{}) uint64 {
	var sum uint64
	for _, v := range values {
		switch vv := v.(type) {
		case string:
			sum += uint64(len(vv))
		case []byte:
			sum += uint64(len(vv))
		case nil:
			sum += 8
		default:
			sum += 8
		}
	}
	return sum
}

// LoadSnapshot loads rows from Oracle and pushes abstract.ChangeItem batches via pusher.
func (l *loader) LoadSnapshot(ctx context.Context, pusher abstract.Pusher, sql string) error {
	rawValues, err := createRawValues(l.table)
	if err != nil {
		return xerrors.Errorf("Can't create raw values for table '%v': %w", l.table.OracleSQLName(), err)
	}

	batch := make([]abstract.ChangeItem, 0, rowsInBatch)
	var batchBytes uint64
	batchTime := time.Now()
	queryErr := oracle_common.PDBQueryGlobal(
		l.config,
		l.sqlxDB,
		ctx,
		func(ctx context.Context, connection *sqlx.Conn) error {
			// Wrap the fetch in an idle watchdog: if no row arrives within
			// ociFetchIdleTimeout, cancel the fetch context so the upper retry
			// loop can re-attempt the part. NOTE: the Oracle driver only
			// unblocks rows.Next() on ctx.Done() if it honours cancellation at
			// the CGO/OCI layer; if not, the cancel will be observed only once
			// the underlying syscall returns (e.g. on TCP keepalive break).
			fetchCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			watchdog := time.AfterFunc(ociFetchIdleTimeout, func() {
				l.logger.Warn("OCI fetch idle timeout exceeded, cancelling fetch",
					log.String("table", l.table.OracleSQLName()),
					log.Duration("idle_timeout", ociFetchIdleTimeout))
				cancel()
			})
			defer watchdog.Stop()

			rows, err := connection.QueryContext(fetchCtx, sql)
			if err != nil {
				return xerrors.Errorf("Can't select table '%v': %w", l.table.OracleSQLName(), err)
			}
			defer rows.Close()
			for rows.Next() {
				watchdog.Reset(ociFetchIdleTimeout)
				if err := rows.Scan(rawValues...); err != nil {
					return xerrors.Errorf("Can't scan values for table '%v': %w", l.table.OracleSQLName(), err)
				}

				columnValues, err := buildChangeItemValues(l.table, rawValues)
				if err != nil {
					return xerrors.Errorf("Can't cast values for table '%v': %w", l.table.OracleSQLName(), err)
				}
				// batchBytes drives the byte-based flush below, so it must be tracked unconditionally
				// — not only when metrics are wired up.
				batchBytes += estimateRowSize(columnValues)
				item := buildInsertChangeItem(l.table, columnValues, l.position)
				batch = append(batch, item)

				if len(batch) >= rowsInBatch || batchBytes >= batchBytesLimit {
					if err := l.pushBatch(batch, batchBytes, &batchTime, pusher); err != nil {
						return xerrors.Errorf("failed to push to target: %w", err)
					}
					batch = make([]abstract.ChangeItem, 0, rowsInBatch)
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
		if err := l.pushBatch(batch, batchBytes, &batchTime, pusher); err != nil {
			return xerrors.Errorf("failed to push to target: %w", err)
		}
	}

	return nil
}

func buildInsertChangeItem(
	table *oracle_schema.Table,
	values []interface{},
	position *oracle_common.LogPosition,
) abstract.ChangeItem {
	tableSchema, _ := table.ToOldTable()
	columnNames := make([]string, table.ColumnsCount())
	for i := 0; i < table.ColumnsCount(); i++ {
		columnNames[i] = table.OracleColumn(i).Name()
	}
	var commitTime uint64
	var lsn uint64
	if position != nil {
		commitTime = uint64(position.Timestamp().UnixNano())
		lsn = position.SCN()
	}
	//nolint:exhaustivestruct
	return abstract.ChangeItem{
		Kind:         abstract.InsertKind,
		Schema:       table.Schema(),
		Table:        table.Name(),
		TableSchema:  tableSchema,
		ColumnNames:  columnNames,
		ColumnValues: values,
		CommitTime:   commitTime,
		LSN:          lsn,
	}
}

func (l *loader) pushBatch(batch []abstract.ChangeItem, batchBytes uint64, batchTime *time.Time, pusher abstract.Pusher) error {
	if err := pusher(batch); err != nil {
		return xerrors.Errorf("failed to push a batch of %d items: %w", len(batch), err)
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
