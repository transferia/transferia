package dblog

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/dblog"
	"github.com/transferia/transferia/pkg/dblog/tablequery"
	"go.ytsaurus.tech/library/go/core/log"
)

type queriableStorage interface {
	abstract.SizeableStorage
	tablequery.TableQueryable
}

type storageCallbacks struct {
	onProgressReset         func()
	chunkCompletionCallback func(uint64)
	betweenWatermarks       []func()
}

type Storage struct {
	logger log.Logger

	src       abstract.Source
	pgStorage queriableStorage
	conn      *pgxpool.Pool

	chunkSize uint64

	transferID   string
	represent    dblog.ChangeItemConverter
	keeperSchema string
	callbacks    storageCallbacks
}

// StorageOption configures optional DBLog callbacks.
type StorageOption func(*Storage)

// WithOnProgressReset sets the callback run when a table is read from the beginning.
func WithOnProgressReset(callback func()) StorageOption {
	return func(s *Storage) { s.callbacks.onProgressReset = callback }
}

// WithChunkCompletionCallback sets the callback run after a chunk is pushed and its watermark is saved.
func WithChunkCompletionCallback(callback func(uint64)) StorageOption {
	return func(s *Storage) { s.callbacks.chunkCompletionCallback = callback }
}

// WithBetweenWatermarks adds callbacks run between the low and high watermarks of non-empty chunks.
func WithBetweenWatermarks(callbacks ...func()) StorageOption {
	return func(s *Storage) { s.callbacks.betweenWatermarks = append(s.callbacks.betweenWatermarks, callbacks...) }
}

func NewStorage(
	logger log.Logger,
	src abstract.Source,
	pgStorage queriableStorage,
	conn *pgxpool.Pool,
	chunkSize uint64,
	transferID string,
	keeperSchema string,
	represent dblog.ChangeItemConverter,
	opts ...StorageOption,
) (abstract.Storage, error) {
	s := &Storage{
		logger:       log.With(logger, log.Any("component", "dblog")),
		src:          src,
		pgStorage:    pgStorage,
		conn:         conn,
		chunkSize:    chunkSize,
		transferID:   transferID,
		keeperSchema: keeperSchema,
		represent:    represent,
		callbacks: storageCallbacks{
			onProgressReset:         nil,
			chunkCompletionCallback: nil,
			betweenWatermarks:       nil,
		},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

func (s *Storage) Close() {
	s.pgStorage.Close()
}

func (s *Storage) Ping() error {
	return s.pgStorage.Ping()
}

func (s *Storage) LoadTable(ctx context.Context, tableDescr abstract.TableDescription, pusher abstract.Pusher) error {
	pkColNames, err := dblog.ResolvePrimaryKeyColumns(ctx, s.pgStorage, tableDescr.ID(), CheckTypeCompatibility)
	if err != nil {
		return xerrors.Errorf("unable to get primary key: %w", err)
	}

	chunkSize := s.chunkSize

	if chunkSize == 0 {
		chunkSize, err = dblog.InferChunkSize(s.pgStorage, tableDescr.ID(), dblog.DefaultChunkSizeInBytes)
		if err != nil {
			return xerrors.Errorf("unable to generate chunk size: %w", err)
		}
		s.logger.Infof("Storage.LoadTable - inferred chunkSize: %d", chunkSize)
	} else {
		s.logger.Infof("Storage.LoadTable - from config chunkSize: %d", chunkSize)
	}

	pgSignalTable, err := NewPgSignalTable(ctx, s.conn, s.logger, s.transferID, s.keeperSchema)
	if err != nil {
		return xerrors.Errorf("unable to create signal table: %w", err)
	}

	tableQuery := tablequery.NewTableQuery(tableDescr.ID(), true, tableDescr.Filter, 0, chunkSize)
	s.logger.Infof("Storage.LoadTable - tableQuery: %v", tableQuery)
	lowBound, err := pgSignalTable.readLowBound(ctx, tableDescr.ID())
	if err != nil {
		return err
	}
	s.logger.Infof("Storage.LoadTable - lowBound: %v", lowBound)
	if len(lowBound) == 0 && s.callbacks.onProgressReset != nil {
		s.callbacks.onProgressReset()
	}

	iterator, err := dblog.NewIncrementalIterator(
		s.logger,
		s.pgStorage,
		tableQuery,
		pgSignalTable,
		s.represent,
		pkColNames,
		lowBound,
		chunkSize,
		s.callbacks.betweenWatermarks...,
	)
	if err != nil {
		return xerrors.Errorf("unable to build iterator, err: %w", err)
	}

	items, err := iterator.Next(ctx)
	if err != nil {
		return xerrors.Errorf("failed to do initial iteration: %w", err)
	}

	s.logger.Infof("Storage.LoadTable - first iteration done, extacted items: %d", len(items))

	chunk, err := dblog.ResolveChunkMapFromArr(items, pkColNames, s.represent)
	if err != nil {
		return xerrors.Errorf("failed to resolve chunk: %w", err)
	}

	asyncSink := dblog.NewIncrementalAsyncSink(
		ctx,
		s.logger,
		pgSignalTable,
		tableDescr.ID(),
		iterator,
		pkColNames,
		chunk,
		s.represent,
		func() { s.src.Stop() },
		pusher,
		s.callbacks.chunkCompletionCallback,
	)

	err = s.src.Run(asyncSink)
	if err != nil {
		s.src.Stop()
		return xerrors.Errorf("unable to run worker: %w", err)
	}

	return nil
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return s.pgStorage.TableSchema(ctx, table)
}

func (s *Storage) TableList(filter abstract.IncludeTableList) (abstract.TableMap, error) {
	return s.pgStorage.TableList(filter)
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.pgStorage.ExactTableRowsCount(table)
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.pgStorage.EstimateTableRowsCount(table)
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	return s.pgStorage.TableExists(table)
}
