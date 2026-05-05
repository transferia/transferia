package greenplum

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/greenplum/gpfdist"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/slicesx"
	"golang.org/x/sync/errgroup"
)

type GpfdistTableSink struct {
	// pipesWriters used to push data by theirs `.Write()` method.
	pipesWriters []*gpfdist.PipeWriter
	gpfdists     []*gpfdistbin.Gpfdist

	extTableTxCloser gpfdistbin.TxCloser // extTableTxCloser manages external table writing transaction closing.

	extTableWriterRes      gpfdistbin.RunResult
	extTableWriterFinished <-chan struct{}
	extTableWriterCancel   context.CancelFunc
}

// commitTransaction commits the open transaction after successful load. Called when DoneTableLoad is processed.
func (s *GpfdistTableSink) commitTransaction(ctx context.Context) error {
	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	rollbacks.Add(func() { s.extTableTxCloser.Rollback(ctx) })

	timeout := 10 * time.Minute
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	logger.Log.Infof("Waiting %s for external writer to complete before commit", timeout.String())

	select {
	case <-s.extTableWriterFinished:
	case <-timer.C:
		s.extTableWriterCancel()
		return xerrors.Errorf("external writer is not stopped within %s timeout and was canceled forcefully", timeout.String())
	}
	res := s.extTableWriterRes
	if res.Err != nil {
		return xerrors.Errorf("external writer failed: %w", res.Err)
	}

	if err := s.extTableTxCloser.Commit(ctx); err != nil {
		return xerrors.Errorf("unable to commit transaction: %w", err)
	}
	rollbacks.Cancel()
	logger.Log.Infof("Transaction committed successfully - %d rows inserted", res.Rows)
	return nil
}

func (s *GpfdistTableSink) Close(ctx context.Context, needCommit bool) error {
	var multiErr error
	pipesRows := int64(0)
	logger.Log.Info("Stopping pipes writers")
	for _, writer := range s.pipesWriters {
		rows, err := writer.Stop()
		if err != nil {
			multiErr = errors.Join(multiErr, xerrors.Errorf("pipe writer stop error: %w", err))
		}
		pipesRows += rows
	}

	if needCommit {
		logger.Log.Info("Pipes writers stopped, stopping external table writer")
		if err := s.commitTransaction(ctx); err != nil {
			multiErr = errors.Join(multiErr, xerrors.Errorf("tx commit error: %w", err))
		}
	}
	rollbackCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	s.extTableTxCloser.Rollback(rollbackCtx) // Does nothing after commit, no need to check `needCommit`.

	logger.Log.Info("External table writer stopped, stopping gpfdists")
	for _, gpfd := range s.gpfdists {
		if err := gpfd.Stop(); err != nil {
			multiErr = errors.Join(multiErr, xerrors.Errorf("gpfdist stop error: %w", err))
		}
	}
	return multiErr
}

func (s *GpfdistTableSink) Push(items []abstract.ChangeItem) error {
	lines := make([][]byte, len(items))
	for i, item := range items {
		if item.Kind != abstract.InsertKind {
			return xerrors.Errorf("unexpected item kind %s", string(item.Kind))
		}
		if len(item.ColumnValues) != 1 {
			return xerrors.Errorf("unexpected item with %d values", len(item.ColumnValues))
		}
		line, ok := item.ColumnValues[0].([]byte)
		if !ok || len(line) == 0 {
			return xerrors.Errorf("expected item's value to be []byte, got '%T' or empty []byte", item.ColumnValues[0])
		}
		lines[i] = line
	}
	chunks := slicesx.SplitToChunks(lines, len(s.pipesWriters))
	eg := errgroup.Group{}
	for i, writer := range s.pipesWriters {
		eg.Go(func() error {
			return writer.Write(chunks[i])
		})
	}
	return eg.Wait()
}

func InitGpfdistTableSink(
	table abstract.TableID, tableSchema *abstract.TableSchema, localAddr net.IP, conn *pgxpool.Pool, params gpfdistbin.GpfdistParams, tmpObjectsSuffix string,
) (*GpfdistTableSink, error) {
	if params.ThreadsCount <= 0 {
		return nil, xerrors.Errorf("number of threads is not positive (%d)", params.ThreadsCount)
	}
	logger.Log.Infof("Creating %d-threaded gpfdist table sink", params.ThreadsCount)

	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	var err error
	mode := gpfdistbin.ImportTable

	// Step 1. Init gpfdist binaries.
	gpfdists := make([]*gpfdistbin.Gpfdist, params.ThreadsCount)
	locations := make([]string, params.ThreadsCount)
	for i := range gpfdists {
		gpfdists[i], err = gpfdistbin.InitGpfdist(params, localAddr, mode, i)
		if err != nil {
			return nil, xerrors.Errorf("unable to init gpfdist: %w", err)
		}
		locations[i] = gpfdists[i].Location()
		logger.Log.Infof("Gpfdist for sink initialized")
	}

	// Step 2. Run background export through external table.
	// Async run transaction, which is kept open until DoneTableLoad (commit) or Close (rollback).
	ctx, extTableWriterCancel := context.WithCancel(context.Background())
	rollbacks.Add(extTableWriterCancel)
	extTableTxCloser, extTableWriterCh := gpfdistbin.RunExternalTableTransaction(
		ctx, mode.ToExternalTableMode(), table, tableSchema,
		locations, conn, params.ServiceSchema, tmpObjectsSuffix,
	)

	// Step3. Run PipesWriters which would asyncly serve theirs `.Write()` method calls.
	pipesWriters := make([]*gpfdist.PipeWriter, params.ThreadsCount)
	for i := range gpfdists {
		pipesWriters[i], err = gpfdist.InitPipeWriter(gpfdists[i])
		if err != nil {
			return nil, xerrors.Errorf("unable to init pipes writer: %w", err)
		}
	}

	rollbacks.Cancel()

	extTableWriterFinished := make(chan struct{})
	sink := &GpfdistTableSink{
		pipesWriters:           pipesWriters,
		gpfdists:               gpfdists,
		extTableTxCloser:       extTableTxCloser,
		extTableWriterRes:      gpfdistbin.RunResult{Rows: 0, Err: xerrors.New("ext writer result not received")},
		extTableWriterFinished: extTableWriterFinished,
		extTableWriterCancel:   extTableWriterCancel,
	}
	go func() {
		sink.extTableWriterRes = <-extTableWriterCh
		close(extTableWriterFinished)
	}()
	return sink, nil
}
