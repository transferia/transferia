//go:build !disable_greenplum_provider

package greenplum

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.Sinker = (*GpfdistSink)(nil)

type GpfdistSink struct {
	dst  *GpDestination
	conn *pgxpool.Pool

	tableSinks   map[abstract.TableID]*GpfdistTableSink
	tableSinksMu sync.RWMutex
	pgCoordSink  abstract.Sinker
	localAddr    net.IP
}

// Close closes and removes all tableSinks.
func (s *GpfdistSink) Close() error {
	s.tableSinksMu.Lock()
	defer s.tableSinksMu.Unlock()
	var errors []error
	for _, tableSink := range s.tableSinks {
		if err := tableSink.Close(); err != nil {
			errors = append(errors, err)
		}
	}
	s.tableSinks = nil
	if len(errors) > 0 {
		return xerrors.Errorf("unable to stop %d/%d gpfdist tableSinks: %w", len(errors), len(s.tableSinks), multierr.Combine(errors...))
	}
	return nil
}

func (s *GpfdistSink) getTableSink(table abstract.TableID) (*GpfdistTableSink, bool) {
	s.tableSinksMu.RLock()
	defer s.tableSinksMu.RUnlock()
	tableSink, ok := s.tableSinks[table]
	return tableSink, ok
}

func (s *GpfdistSink) removeTableSink(table abstract.TableID) error {
	s.tableSinksMu.Lock()
	defer s.tableSinksMu.Unlock()
	tableSink, ok := s.tableSinks[table]
	if !ok {
		return xerrors.Errorf("sink for table %s not exists", table)
	}
	err := tableSink.Close()
	delete(s.tableSinks, table)
	return err
}

func (s *GpfdistSink) getOrCreateTableSink(table abstract.TableID, schema *abstract.TableSchema) error {
	s.tableSinksMu.Lock()
	defer s.tableSinksMu.Unlock()
	if _, ok := s.tableSinks[table]; ok {
		return nil
	}

	tableSink, err := InitGpfdistTableSink(table, schema, s.localAddr, s.conn, s.dst)
	if err != nil {
		return xerrors.Errorf("unable to init sink for table %s: %w", table, err)
	}
	s.tableSinks[table] = tableSink
	return nil
}

func (s *GpfdistSink) pushToTableSink(table abstract.TableID, items []*abstract.ChangeItem) error {
	s.tableSinksMu.RLock()
	defer s.tableSinksMu.RUnlock()
	tableSink, ok := s.tableSinks[table]
	if !ok {
		return xerrors.Errorf("sink for table %s not exists", table)
	}
	return tableSink.Push(items)
}

func (s *GpfdistSink) Push(items []abstract.ChangeItem) error {
	// systemKindCtx is used to cancel system actions (cleanup or init table load)
	// and do not applies to inserts.
	systemKindCtx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	insertItems := make(map[abstract.TableID][]*abstract.ChangeItem)
	for _, item := range items {
		table := item.TableID()
		switch item.Kind {
		case abstract.InitTableLoad:
			if err := s.getOrCreateTableSink(table, item.TableSchema); err != nil {
				return xerrors.Errorf("unable to start sink for table %s: %w", table, err)
			}
		case abstract.DoneTableLoad:
			if err := s.removeTableSink(table); err != nil {
				return xerrors.Errorf("unable to stop sink for table %s: %w", table, err)
			}
		case abstract.InsertKind:
			insertItems[table] = append(insertItems[table], &item)
		case abstract.TruncateTableKind, abstract.DropTableKind:
			if err := s.processCleanupChangeItem(systemKindCtx, &item); err != nil {
				return xerrors.Errorf("failed to process %s: %w", item.Kind, err)
			}
		case abstract.InitShardedTableLoad:
			if err := s.processInitTableLoad(systemKindCtx, &item); err != nil {
				return xerrors.Errorf("sinker failed to initialize table load for table %s: %w", item.PgName(), err)
			}
		case abstract.DoneShardedTableLoad, abstract.SynchronizeKind:
			// do nothing
		default:
			return xerrors.Errorf("item kind %s is not supported", item.Kind)
		}
	}

	for table, items := range insertItems {
		if err := s.pushToTableSink(table, items); err != nil {
			return xerrors.Errorf("unable to push to table %s: %w", table, err)
		}
	}
	return nil
}

func (s *GpfdistSink) processInitTableLoad(ctx context.Context, ci *abstract.ChangeItem) error {
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return xerrors.Errorf("failed to BEGIN a transaction on sink %s: %w", Coordinator(), err)
	}
	rollbacks.Add(loggingRollbackTxFunc(ctx, tx))

	if csq := postgres.CreateSchemaQueryOptional(ci.PgName()); len(csq) > 0 {
		if _, err := tx.Exec(ctx, csq); err != nil {
			logger.Log.Warn("Failed to execute CREATE SCHEMA IF NOT EXISTS query at table load initialization.", log.Error(err))
		}
	}

	if err := ensureTargetRandDistExists(ctx, ci, tx.Conn()); err != nil {
		return xerrors.Errorf("failed to ensure target table existence: %w", err)
	}

	if err := recreateTmpTable(ctx, ci, tx.Conn(), abstract.PgName(temporaryTable(ci.Schema, ci.Table))); err != nil {
		return xerrors.Errorf("failed to (re)create the temporary data transfer table: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return xerrors.Errorf("failed to COMMIT a transaction on sink %s: %w", Coordinator(), err)
	}
	rollbacks.Cancel()
	return nil
}

func (s *GpfdistSink) pushChangeItemsToPgCoordinator(changeItems []abstract.ChangeItem) error {
	if err := s.pgCoordSink.Push(changeItems); err != nil {
		return xerrors.Errorf("failed to execute push to Coordinator: %w", err)
	}
	return nil
}

func (s *GpfdistSink) processCleanupChangeItem(ctx context.Context, changeItem *abstract.ChangeItem) error {
	if err := s.pushChangeItemsToPgCoordinator([]abstract.ChangeItem{*changeItem}); err != nil {
		return xerrors.Errorf("failed to execute single push on sinker %s: %w", Coordinator().String(), err)
	}
	return nil
}

func NewGpfdistSink(dst *GpDestination, registry metrics.Registry, lgr log.Logger, transferID string) (*GpfdistSink, error) {
	storage := NewStorage(dst.ToGpSource(), registry)
	conn, err := coordinatorConnFromStorage(storage)
	if err != nil {
		return nil, xerrors.Errorf("unable to init coordinator conn: %w", err)
	}
	localAddr, err := localAddrFromStorage(storage)
	if err != nil {
		return nil, xerrors.Errorf("unable to get local address: %w", err)
	}
	sinkParams := GpDestinationToPgSinkParamsRegulated(dst)
	sinks := newPgSinks(storage, lgr, transferID, registry)
	ctx := context.Background()
	pgCoordSinker, err := sinks.PGSink(ctx, Coordinator(), *sinkParams)
	if err != nil {
		return nil, xerrors.Errorf("failed to connect to Coordinator: %w", err)
	}

	return &GpfdistSink{
		dst:          dst,
		conn:         conn,
		tableSinks:   make(map[abstract.TableID]*GpfdistTableSink),
		tableSinksMu: sync.RWMutex{},
		pgCoordSink:  pgCoordSinker,
		localAddr:    localAddr,
	}, nil
}
