package gpfdist

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
	"go.ytsaurus.tech/library/go/core/log"
)

type PipeReader struct {
	ctx       context.Context
	gpfdist   *gpfdistbin.Gpfdist
	template  abstract.ChangeItem
	batchSize int
	pushedCnt atomic.Int64
	errCh     chan error
}

func (r *PipeReader) readFromPipe(reader io.Reader, pusher abstract.Pusher) (int64, error) {
	batch := make([]abstract.ChangeItem, 0, r.batchSize)
	pushedCnt, quotesCnt := int64(0), 0
	var lineParts []string

	scanner := bufio.NewReader(reader)
	var readErr error
	for readErr != io.EOF {
		var line string
		line, readErr = scanner.ReadString('\n')
		if readErr != nil && readErr != io.EOF {
			return pushedCnt, xerrors.Errorf("unable to read string: %w", readErr)
		}
		if readErr == io.EOF && len(line) == 0 {
			continue // On io.EOF `line` may be either empty or not.
		}
		quotesCnt += strings.Count(line, `"`)
		if quotesCnt%2 != 0 {
			// Quotes not paired, got not full line.
			lineParts = append(lineParts, line)
			continue
		}

		// Quotes paired, add line to batch.
		quotesCnt = 0
		line = strings.Join(lineParts, "") + line
		lineParts = nil
		batch = append(batch, r.itemFromTemplate([]any{line}))
		if len(batch) == r.batchSize {
			if err := pusher(batch); err != nil {
				return pushedCnt, xerrors.Errorf("unable to push %d-elements batch: %w", len(batch), err)
			}
			pushedCnt += int64(r.batchSize)
			batch = make([]abstract.ChangeItem, 0, r.batchSize)
		}
	}
	if len(batch) > 0 {
		if err := pusher(batch); err != nil {
			return pushedCnt, xerrors.Errorf("unable to push %d-elements batch (last): %w", len(batch), err)
		}
		pushedCnt += int64(len(batch))
	}

	if lineParts != nil {
		return pushedCnt, xerrors.New("got non-paired quotes")
	}
	return pushedCnt, nil
}

func (r *PipeReader) itemFromTemplate(columnValues []any) abstract.ChangeItem {
	item := r.template
	item.ColumnValues = columnValues
	return item
}

func (r *PipeReader) Stop(timeout time.Duration) (int64, error) {
	var cancel context.CancelFunc
	r.ctx, cancel = context.WithTimeout(r.ctx, timeout)
	defer cancel()
	err := <-r.errCh
	return r.pushedCnt.Load(), err
}

// Run should be called once per PipeReader life, it is not guaranteed that more calls will proceed.
func (r *PipeReader) Run(pusher abstract.Pusher) {
	r.errCh <- r.runImpl(pusher)
}

func (r *PipeReader) runImpl(pusher abstract.Pusher) error {
	pipe, err := r.gpfdist.OpenPipe()
	if err != nil {
		return xerrors.Errorf("unable to open pipe: %w", err)
	}
	defer func() {
		if err := pipe.Close(); err != nil {
			logger.Log.Error(fmt.Sprintf("Unable to close pipe %s", pipe.Name()), log.Error(err))
		}
	}()
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		curRows, err := r.readFromPipe(pipe, pusher)
		r.pushedCnt.Add(curRows)
		errCh <- err
	}()
	select {
	case err := <-errCh:
		return err
	case <-r.ctx.Done():
		return xerrors.New("context is done before PipeReader worker")
	}
}

func NewPipeReader(gpfdist *gpfdistbin.Gpfdist, template abstract.ChangeItem, batchSize int) *PipeReader {
	return &PipeReader{
		ctx:       context.Background(),
		gpfdist:   gpfdist,
		template:  template,
		batchSize: batchSize,
		pushedCnt: atomic.Int64{},
		errCh:     make(chan error, 1),
	}
}
