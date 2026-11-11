package part_uploader

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	ch_driver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/stats"
)

const (
	flushBytesThreshold  = 10 * 1024 * 1024
	insertBytesThreshold = 100 * 1024 * 1024
	flushRowsThreshold   = 16384
)

// shardWriter owns a bounded streaming INSERT on one shard's selected host.
// Write copies rows into
// driver columns; Finish must receive the server acknowledgement before the
// snapshot attempt is verified. No failed INSERT is retried by this writer.
type shardWriter struct {
	mutex                  sync.Mutex
	connection             ch_driver.Conn
	databaseName           string
	includeTransferColumns bool
	insertOptions          clickhouse_go.QueryOption
	metrics                *stats.ChStats // nil on replica copies: logical rows are counted once per shard.
	tableName              string
	insertQuery            string
	encoder                *rowEncoder
	batch                  ch_driver.Batch
	batchContext           context.Context
	cancelBatch            context.CancelFunc
	bufferedBytes          uint64
	bufferedRows           uint64
	insertBytes            uint64
	insertRows             uint64
	poison                 error
	closed                 bool
}

var _ io.Closer = (*shardWriter)(nil)

// newShardWriter creates a streaming INSERT writer for one ClickHouse host.
func newShardWriter(connection ch_driver.Conn, databaseName string, includeTransferColumns bool, insertOptions clickhouse_go.QueryOption, metrics *stats.ChStats) *shardWriter {
	return &shardWriter{
		mutex:                  sync.Mutex{},
		connection:             connection,
		databaseName:           databaseName,
		includeTransferColumns: includeTransferColumns,
		insertOptions:          insertOptions,
		metrics:                metrics,
		tableName:              "",
		insertQuery:            "",
		encoder:                nil,
		batch:                  nil,
		batchContext:           nil,
		cancelBatch:            nil,
		bufferedBytes:          0,
		bufferedRows:           0,
		insertBytes:            0,
		insertRows:             0,
		poison:                 nil,
		closed:                 false,
	}
}

// Write copies rows into native column buffers and may flush them without awaiting the final INSERT acknowledgement.
func (w *shardWriter) Write(tableName string, items []abstract.ChangeItem) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.poison != nil {
		return w.poison
	}
	if w.closed {
		return xerrors.New("snapshot native writer is closed")
	}
	if len(items) == 0 {
		return nil
	}
	defer func() {
		if w.encoder != nil {
			// Driver Append copies values; do not retain the source's last row.
			clear(w.encoder.argumentBuffer)
		}
	}()

	tableChanged := w.tableName != tableName
	needsNewEncoder := w.encoder == nil || w.encoder.sourceSchema != items[0].TableSchema || !slices.Equal(w.encoder.columnNames, items[0].ColumnNames)
	if tableChanged || needsNewEncoder {
		if err := w.finishBatchLocked(); err != nil {
			return xerrors.Errorf("write snapshot rows: %w", err)
		}
		w.insertQuery = buildInsertQuery(w.databaseName, tableName, items[0].ColumnNames, w.includeTransferColumns)
		w.tableName = tableName
		if needsNewEncoder {
			w.encoder = nil
		}
	}

	for chunkBegin := 0; chunkBegin < len(items); {
		if w.batch == nil {
			if err := w.prepareBatch(items[chunkBegin]); err != nil {
				return xerrors.Errorf("write snapshot rows: %w", err)
			}
		}
		chunkEnd := chunkBegin
		var chunkBytes uint64
		for chunkEnd < len(items) {
			chunkBytes += estimateRowBytes(&items[chunkEnd])
			chunkEnd++
			if w.bufferedBytes+chunkBytes >= flushBytesThreshold || w.bufferedRows+uint64(chunkEnd-chunkBegin) >= flushRowsThreshold || w.insertBytes+chunkBytes >= insertBytesThreshold {
				break
			}
		}
		if err := appendRows(w.batch, w.encoder, items[chunkBegin:chunkEnd]); err != nil {
			return xerrors.Errorf("snapshot native writer: %w", w.poisonAndAbortBatch(wrapWriteError("append snapshot INSERT", err)))
		}
		w.bufferedBytes += chunkBytes
		w.insertBytes += chunkBytes
		w.bufferedRows += uint64(chunkEnd - chunkBegin)
		w.insertRows += uint64(chunkEnd - chunkBegin)
		chunkBegin = chunkEnd
		if w.insertBytes >= insertBytesThreshold {
			if err := w.finishBatchLocked(); err != nil {
				return xerrors.Errorf("write snapshot rows: %w", err)
			}
			continue
		}
		if w.bufferedBytes >= flushBytesThreshold || w.bufferedRows >= flushRowsThreshold {
			if err := w.batch.Flush(); err != nil {
				return xerrors.Errorf("snapshot native writer: %w", w.poisonAndAbortBatch(wrapWriteError("flush snapshot INSERT", err)))
			}
			w.bufferedBytes = 0
			w.bufferedRows = 0
		}
	}
	return nil
}

func (w *shardWriter) prepareBatch(sampleRow abstract.ChangeItem) error {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = clickhouse_go.Context(ctx, w.insertOptions)
	// The streaming query can outlive this timeout. Bound each network operation
	// separately rather than giving the whole batch an absolute expiry time.
	stopTimeoutAndWait := startIOTimeout(defaultIOTimeout, cancel)
	batch, err := w.connection.PrepareBatch(ctx, w.insertQuery)
	stopTimeoutAndWait()
	if err != nil {
		cancel()
		return xerrors.Errorf("snapshot native writer: %w", w.poisonAndAbortBatch(wrapWriteError("prepare snapshot INSERT", err)))
	}
	w.batch = batch
	w.batchContext = ctx
	w.cancelBatch = cancel
	if err := ctx.Err(); err != nil {
		return xerrors.Errorf("snapshot native writer: %w", w.poisonAndAbortBatch(wrapWriteError("prepare snapshot INSERT", err)))
	}
	targetColumns := batch.Columns()
	if w.encoder != nil && !w.encoder.canReuseForTargetColumns(targetColumns) {
		w.encoder = nil
	}
	if w.encoder == nil {
		w.encoder, err = newRowEncoder(sampleRow, targetColumns, w.includeTransferColumns)
		if err != nil {
			return xerrors.Errorf("snapshot native writer: %w", w.poisonAndAbortBatch(xerrors.Errorf("prepare snapshot row encoder: %w", err)))
		}
	}
	return nil
}

// Finish completes the INSERT and waits for the server acknowledgement.
func (w *shardWriter) Finish() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.poison != nil {
		return w.poison
	}
	if w.closed {
		return xerrors.New("snapshot native writer is closed")
	}
	if err := w.finishBatchLocked(); err != nil {
		return xerrors.Errorf("finish snapshot INSERT: %w", err)
	}
	return nil
}

func (w *shardWriter) finishBatchLocked() error {
	if w.batch == nil {
		return nil
	}
	// Send's context watchdog interrupts a stalled final acknowledgement.
	stopTimeoutAndWait := startIOTimeout(defaultIOTimeout, w.cancelBatch)
	err := w.batch.Send()
	stopTimeoutAndWait()
	if err == nil {
		err = w.batchContext.Err()
	}
	if err != nil {
		return xerrors.Errorf("snapshot native writer: %w", w.poisonAndAbortBatch(wrapWriteError("send snapshot INSERT", err)))
	}
	if w.metrics != nil {
		w.metrics.Len.Add(int64(w.insertRows))
		w.metrics.Count.Inc()
	}
	w.clearBatchState()
	return nil
}

func (w *shardWriter) poisonAndAbortBatch(err error) error {
	if w.poison == nil {
		w.poison = err
	}
	if w.batch != nil && !w.batch.IsSent() {
		if abortError := w.batch.Abort(); abortError != nil {
			w.poison = errors.Join(w.poison, xerrors.Errorf("abort snapshot INSERT: %w", abortError))
		}
	}
	if w.cancelBatch != nil {
		w.cancelBatch()
	}
	w.clearBatchState()
	return w.poison
}

func (w *shardWriter) clearBatchState() {
	// Do not cancel a successfully sent query: the stock driver's stopped
	// watchdog can still observe cancellation and close a pooled connection.
	// This parentless context has no timer left and can be garbage-collected.
	w.batch = nil
	w.batchContext = nil
	w.cancelBatch = nil
	w.bufferedBytes = 0
	w.bufferedRows = 0
	w.insertBytes = 0
	w.insertRows = 0
}

// Close aborts pending data. Successful publication must call Finish explicitly.
func (w *shardWriter) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true
	var abortError error
	if w.batch != nil && !w.batch.IsSent() {
		abortError = w.batch.Abort()
	}
	if w.cancelBatch != nil {
		w.cancelBatch()
	}
	w.clearBatchState()
	w.encoder = nil
	return errors.Join(abortError, w.connection.Close())
}
