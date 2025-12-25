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

func (s *Storage) GetNextIncrementalState(ctx context.Context, incremental []abstract.IncrementalTable) ([]abstract.IncrementalState, error) {
	conn, err := s.Conn.Acquire(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire a connection: %w", err)
	}
	defer conn.Release()

	tx, txRollbacks, err := BeginTxWithSnapshot(ctx, conn.Conn(), repeatableReadReadOnlyTxOptions, s.ShardedStateLSN, logger.Log)
	if err != nil {
		return nil, xerrors.Errorf("failed to start a transaction: %w", err)
	}
	defer txRollbacks.Do()

	var res []abstract.IncrementalState
	for _, table := range incremental {
		var maxVal interface{}
		var cursorType string
		if err := tx.QueryRow(
			ctx,
			fmt.Sprintf(
				`select pg_typeof("%s") from "%s"."%s" limit 1`,
				table.CursorField,
				table.Namespace,
				table.Name,
			)).Scan(&cursorType); err != nil {
			if err == pgx.ErrNoRows {
				logger.Log.Warn(fmt.Sprintf("unable get type of %s column from table", table.CursorField), log.String("table", table.TableID().Fqtn()), log.Error(err))
				continue
			}
			return nil, xerrors.Errorf("unable get type of %s column from table: %s: %w", table.CursorField, table.TableID(), err)
		}
		st := time.Now()
		initialFilter := ""
		if table.InitialState != "" {
			initialFilter = fmt.Sprintf("where \"%s\" > %s", table.CursorField, table.InitialState)
		}
		nextValueQ := fmt.Sprintf(
			`select "%s" from "%s"."%s" %s order by "%[1]s" desc limit 1`,
			table.CursorField,
			table.Namespace,
			table.Name,
			initialFilter,
		)
		if err := tx.QueryRow(
			ctx,
			nextValueQ,
		).Scan(&maxVal); err != nil {
			if err == pgx.ErrNoRows {
				logger.Log.Warn(fmt.Sprintf("unable get max %s from table", table.CursorField), log.String("table", table.TableID().Fqtn()), log.Error(err))
				continue
			}
			return nil, xerrors.Errorf("unable get max %s from table: %s: %w", table.CursorField, table.TableID(), err)
		}
		columnType := new(abstract.ColSchema)
		columnType.OriginalType = fmt.Sprintf("pg:%v", cursorType)
		columnType.DataType = string(PgTypeToYTType(cursorType))
		repr, err := Represent(maxVal, *columnType)
		if err != nil {
			return nil, xerrors.Errorf("unable to represent value: %w", err)
		}
		res = append(res, abstract.IncrementalState{
			Name:    table.Name,
			Schema:  table.Namespace,
			Payload: abstract.WhereStatement(fmt.Sprintf(`"%s" > %s`, table.CursorField, repr)),
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
