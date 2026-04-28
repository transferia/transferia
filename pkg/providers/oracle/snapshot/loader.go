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
			rows, err := connection.QueryContext(ctx, sql)
			if err != nil {
				return xerrors.Errorf("Can't select table '%v': %w", l.table.OracleSQLName(), err)
			}
			defer rows.Close()

			for rows.Next() {
				if err := rows.Scan(rawValues...); err != nil {
					return xerrors.Errorf("Can't scan values for table '%v': %w", l.table.OracleSQLName(), err)
				}

				columnValues, err := buildChangeItemValues(l.table, rawValues)
				if err != nil {
					return xerrors.Errorf("Can't cast values for table '%v': %w", l.table.OracleSQLName(), err)
				}
				if l.metrics != nil {
					batchBytes += estimateRowSize(columnValues)
				}
				item := buildInsertChangeItem(l.table, columnValues, l.position)
				batch = append(batch, item)

				if len(batch) >= rowsInBatch {
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
