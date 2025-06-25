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
	"github.com/transferia/transferia/pkg/util/slicesx"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/errgroup"
)

type GpfdistTableSink struct {
	// pipesWriters used to push data by theirs `.Write()` method.
	pipesWriters []*gpfdist.PipeWriter
	gpfdists     []*gpfdistbin.Gpfdist

	// stopExtWriter waits for ExtWriter self-stop, and forcely cancels it if `timeout` expires.
	// Expected that ExtWriter will stop by itself after `PipeWriter` stopped and won't be forcely cancelled.
	stopExtWriter func(timeout time.Duration) (int64, error)
}

func (s *GpfdistTableSink) Close() error {
	pipesRows := int64(0)
	logger.Log.Info("Stopping pipes writers")
	for _, writer := range s.pipesWriters {
		rows, err := writer.Stop()
		if err != nil {
			logger.Log.Error("Lines writer stopped with error", log.Error(err))
		}
		pipesRows += rows
	}

	logger.Log.Info("Pipes writers stopped, stopping external table writer")
	tableRows, err := s.stopExtWriter(time.Minute)
	if err != nil {
		logger.Log.Error("External table writer stopped with error", log.Error(err))
	}

	if pipesRows != tableRows {
		logger.Log.Errorf("Lines writer wrote %d lines, while external table writer – %d", pipesRows, tableRows)
	}
	logger.Log.Info("External table writer stopped, stopping gpfdists")

	err = nil
	for _, gpfd := range s.gpfdists {
		err = errors.Join(err, gpfd.Stop())
	}
	if err != nil {
		return xerrors.Errorf("unable to stop gpfdists: %w", err)
	}
	return nil
}

func (s *GpfdistTableSink) Push(items []*abstract.ChangeItem) error {
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
	table abstract.TableID, tableSchema *abstract.TableSchema, localAddr net.IP, conn *pgxpool.Pool, dst *GpDestination, threads int, params gpfdistbin.GpfdistParams,
) (*GpfdistTableSink, error) {
	var err error
	mode := gpfdistbin.ImportTable

	// Step 1. Init gpfdist binaries.
	gpfdists := make([]*gpfdistbin.Gpfdist, threads)
	locations := make([]string, threads)
	for i := range gpfdists {
		gpfdists[i], err = gpfdistbin.InitGpfdist(params, localAddr, mode, i)
		if err != nil {
			return nil, xerrors.Errorf("unable to init gpfdist: %w", err)
		}
		locations[i] = gpfdists[i].Location()
		logger.Log.Debugf("Gpfdist for sink initialized")
	}

	type workerResult struct {
		rows int64
		err  error
	}

	// Step 2. Run background export through external table.
	ctx, cancel := context.WithCancel(context.Background())
	extWriterCh := make(chan workerResult, 1)
	stopExtWriter := func(timeout time.Duration) (int64, error) {
		timer := time.NewTimer(timeout)
		var res workerResult
		select {
		case res = <-extWriterCh:
		case <-timer.C:
			logger.Log.Errorf("External table writer not stopped during %s timeout, force cancelling it", timeout)
			cancel()
			res = <-extWriterCh
		}
		return res.rows, res.err
	}
	go func() {
		defer close(extWriterCh)
		ddlExecutor := gpfdistbin.NewGpfdistDDLExecutor(conn, params.ServiceSchema)
		rows, err := ddlExecutor.RunExternalTableTransaction(
			ctx, mode.ToExternalTableMode(), table, tableSchema, locations,
		)
		extWriterCh <- workerResult{rows: rows, err: err}
		logger.Log.Info("External table writer goroutine stopped")
	}()

	// Step3. Run PipesWriters which would asyncly serve theirs `.Write()` method calls.
	pipesWriters := make([]*gpfdist.PipeWriter, threads)
	for i := range gpfdists {
		pipesWriters[i], err = gpfdist.InitPipeWriter(gpfdists[i])
		if err != nil {
			return nil, xerrors.Errorf("unable to init pipes writer: %w", err)
		}
	}

	return &GpfdistTableSink{
		pipesWriters:  pipesWriters,
		gpfdists:      gpfdists,
		stopExtWriter: stopExtWriter,
	}, nil
}
