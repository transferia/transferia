package greenplum

import (
	"sync"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/library/go/core/xerrors/multierr"
	"github.com/transferria/transferria/pkg/abstract"
)

var _ abstract.Sinker = (*GpfdistSink)(nil)

type GpfdistSink struct {
	dst  *GpDestination
	conn *pgxpool.Pool

	tableSinks   map[abstract.TableID]*GpfdistTableSink
	tableSinksMu sync.RWMutex
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

	tableSink, err := InitGpfdistTableSink(table, schema, s.conn, s.dst)
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
			// TODO: TM-8406.
		case abstract.InitShardedTableLoad, abstract.DoneShardedTableLoad:
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

func NewGpfdistSink(dst *GpDestination, registry metrics.Registry) (*GpfdistSink, error) {
	conn, err := coordinatorConnFromStorage(NewStorage(dst.ToGpSource(), registry))
	if err != nil {
		return nil, xerrors.Errorf("unable to init coordinator conn: %w", err)
	}
	return &GpfdistSink{
		dst:          dst,
		conn:         conn,
		tableSinks:   make(map[abstract.TableID]*GpfdistTableSink),
		tableSinksMu: sync.RWMutex{},
	}, nil
}
