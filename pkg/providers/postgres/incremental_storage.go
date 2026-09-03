package postgres

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

// ensure that Storage is indeed incremental
var _ abstract.IncrementalStorage = new(Storage)

var repeatableReadReadOnlyTxOptions pgx.TxOptions = pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly, DeferrableMode: pgx.NotDeferrable}

const queryTemplate = `
SELECT format_type(a.atttypid, a.atttypmod)
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1
  AND c.relname = $2
  AND a.attname = $3
  AND a.attnum > 0
  AND NOT a.attisdropped;
`

func (s *Storage) GetNextIncrementalState(ctx context.Context, incremental []abstract.IncrementalTable) ([]abstract.IncrementalState, error) {
	logger.Log.Info("started GetNextIncrementalState")

	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire a connection: %w", err)
	}
	defer conn.Release()

	logger.Log.Info("connection acquired")

	tx, txRollbacks, err := BeginTxWithSnapshot(ctx, conn.Conn(), repeatableReadReadOnlyTxOptions, s.ShardedStateLSN, logger.Log)
	if err != nil {
		return nil, xerrors.Errorf("failed to start a transaction: %w", err)
	}
	defer txRollbacks.Do()

	logger.Log.Info("transaction started")

	var res []abstract.IncrementalState
	for _, table := range incremental {
		logger.Log.Infof("started to handle table %q", table.Name)
		logger.Log.Info("querying cursor field type", log.String("query", queryTemplate), log.Any("params", []string{table.Namespace, table.Name, table.CursorField}))

		var cursorType string
		err := tx.QueryRow(
			ctx,
			queryTemplate,
			table.Namespace,
			table.Name,
			table.CursorField,
		).Scan(&cursorType)
		if err != nil {
			return nil, xerrors.Errorf("unable to get type of column %s from table: %s: %w", table.CursorField, table.TableID(), err)
		}

		logger.Log.Infof("found type of field-cursor: %s", cursorType)

		st := time.Now()

		cursor := pgx.Identifier{table.CursorField}.Sanitize()
		relation := pgx.Identifier{table.Namespace, table.Name}.Sanitize()

		var row pgx.Row
		var nextValueQ string

		if table.InitialState != "" {
			nextValueQ = fmt.Sprintf(`SELECT %[1]s
				FROM %[2]s
				WHERE %[1]s IS NOT NULL
				  AND %[1]s > $1::text::%[3]s
				ORDER BY %[1]s DESC
				LIMIT 1`,
				cursor,     // %[1]s
				relation,   // %[2]s
				cursorType, // %[3]s
			)
			logger.Log.Info("built query for next incremental state", log.String("query", nextValueQ))
			row = tx.QueryRow(ctx, nextValueQ, table.InitialState)
		} else {
			nextValueQ = fmt.Sprintf(
				`SELECT %[1]s
				FROM %[2]s
				WHERE %[1]s IS NOT NULL
				ORDER BY %[1]s DESC
				LIMIT 1`,
				cursor,
				relation,
			)
			logger.Log.Info("built query for next incremental state", log.String("query", nextValueQ))
			row = tx.QueryRow(ctx, nextValueQ)
		}

		logger.Log.Info("QueryRow returned result")

		var maxVal any
		if err := row.Scan(&maxVal); err != nil {
			if xerrors.Is(err, pgx.ErrNoRows) {
				logger.Log.Warn(fmt.Sprintf("unable get max %s from table", table.CursorField), log.String("table", table.TableID().Fqtn()), log.Error(err))
				continue
			}
			return nil, xerrors.Errorf("unable get max %s from table: %s: %w", table.CursorField, table.TableID(), err)
		}

		logger.Log.Infof("got maxVal, maxVal: %v", maxVal)

		columnType := new(abstract.ColSchema)
		columnType.OriginalType = fmt.Sprintf("pg:%v", cursorType)
		columnType.DataType = string(PgTypeToYTType(cursorType))
		repr, err := Represent(maxVal, *columnType)
		if err != nil {
			return nil, xerrors.Errorf("unable to represent value: %w", err)
		}
		res = append(res, abstract.IncrementalState{
			Name:   table.Name,
			Schema: table.Namespace,
			Payload: abstract.WhereStatement(
				fmt.Sprintf(`%s > %s`, cursor, repr),
			),
		})

		logger.Log.Infof(
			"fetch next incremental state %s for: %s, value: %v: %v, in: %v",
			nextValueQ,
			table.TableID().Fqtn(),
			table.CursorField,
			repr,
			time.Since(st),
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, xerrors.Errorf("failed to COMMIT a transaction: %w", err)
	}
	txRollbacks.Cancel()
	return res, nil
}

func SetInitialState(tables []abstract.TableDescription, incrementalTables []abstract.IncrementalTable) []abstract.TableDescription {
	result := slices.Clone(tables)
	for i, table := range result {
		if table.Filter != "" || table.Offset != 0 {
			// table already contains predicate
			continue
		}
		for _, incremental := range incrementalTables {
			if incremental.CursorField == "" || incremental.InitialState == "" {
				continue
			}
			if table.ID() == incremental.TableID() {
				result[i] = abstract.TableDescription{
					Name:   incremental.Name,
					Schema: incremental.Namespace,
					Filter: abstract.WhereStatement(fmt.Sprintf(`"%s" > %s`, incremental.CursorField, incremental.InitialState)),
					EtaRow: 0,
					Offset: 0,
				}
			}
		}
	}
	return result
}

func (s *Storage) BuildArrTableDescriptionWithIncrementalState(tables []abstract.TableDescription, incrementalTables []abstract.IncrementalTable) []abstract.TableDescription {
	return SetInitialState(tables, incrementalTables)
}
