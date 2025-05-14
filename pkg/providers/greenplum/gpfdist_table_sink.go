package greenplum

import (
	"context"
	"net"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/greenplum/gpfdist"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
	"go.ytsaurus.tech/library/go/core/log"
)

type GpfdistTableSink struct {
	// pipesWriter used to push data by its `.Write()` method.
	pipesWriter *gpfdist.PipeWriter
	gpfdist     *gpfdistbin.Gpfdist

	// stopExtWriter waits for ExtWriter self-stop, and forcely cancels it if `timeout` expires.
	// Expected that ExtWriter will stop by itself after `PipeWriter` stopped and won't be forcely cancelled.
	stopExtWriter func(timeout time.Duration) (int64, error)
}

func (s *GpfdistTableSink) Close() error {
	logger.Log.Info("Stopping pipes writer")
	pipesRows, err := s.pipesWriter.Stop()
	if err != nil {
		logger.Log.Error("Lines writer stopped with error", log.Error(err))
	}

	logger.Log.Info("Pipes writer stopped, stopping external table writer")
	tableRows, err := s.stopExtWriter(time.Minute)
	if err != nil {
		logger.Log.Error("External table writer stopped with error", log.Error(err))
	}

	if pipesRows != tableRows {
		logger.Log.Errorf("Lines writer wrote %d lines, while external table writer – %d", pipesRows, tableRows)
	}
	logger.Log.Info("External table writer stopped, stopping gpfdist")
	return s.gpfdist.Stop()
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
	if err := s.pipesWriter.Write(lines); err != nil {
		return xerrors.Errorf("unable to push %d lines to pipe: %w", len(lines), err)
	}
	return nil
}

func InitGpfdistTableSink(
	table abstract.TableID, tableSchema *abstract.TableSchema, localAddr net.IP, conn *pgxpool.Pool, dst *GpDestination,
) (*GpfdistTableSink, error) {
	// Init gpfdist binary.
	gpfd, err := gpfdistbin.InitGpfdist(dst.GpfdistParams, localAddr, gpfdistbin.ImportTable, conn)
	if err != nil {
		return nil, xerrors.Errorf("unable to init gpfdist: %w", err)
	}
	logger.Log.Debugf("Gpfdist for sink initialized")

	type workerResult struct {
		rows int64
		err  error
	}

	// Run background export through external table.
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
		rows, err := gpfd.RunExternalTableTransaction(ctx, table, tableSchema)
		extWriterCh <- workerResult{rows: rows, err: err}
		logger.Log.Info("External table writer goroutine stopped")
	}()

	// Run PipesWriter that would asyncly serve its `.Write()` method calls.
	pipesWriter, err := gpfdist.InitPipeWriter(gpfd)
	if err != nil {
		return nil, xerrors.Errorf("unable to init pipes writer: %w", err)
	}

	return &GpfdistTableSink{
		pipesWriter:   pipesWriter,
		gpfdist:       gpfd,
		stopExtWriter: stopExtWriter,
	}, nil
}
