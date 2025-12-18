package gpfdist

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	fileBlockSize = 50 * humanize.MiByte // Size of one file block (used when reading pipe).
)

type PipeReader struct {
	ctx       context.Context
	cancel    context.CancelFunc
	gpfdist   *gpfdistbin.Gpfdist
	template  abstract.ChangeItem
	pushedCnt int64
	err       error
	done      chan struct{}
}

func (r *PipeReader) readFromPipe(file *os.File, pusher abstract.Pusher) (int64, error) {
	pushedCnt := int64(0)
	splitter := NewLinesSplitter()
	batch := make([]abstract.ChangeItem, 0, 5000)

	buffer := make([]byte, fileBlockSize)
	for {
		n, err := io.ReadAtLeast(file, buffer[:fileBlockSize], fileBlockSize)
		if n == 0 && err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			logger.Log.Errorf("Unable to read pipe %s: %s", file.Name(), err.Error())
			return pushedCnt, xerrors.Errorf("unable to read pipe: %w", err)
		}
		logger.Log.Debugf("Read %d bytes from pipe", n)
		splitted := splitter.Do(buffer[:n])
		for _, line := range splitted {
			batch = append(batch, r.itemFromTemplate(line))
		}
		if len(batch) > 0 {
			if err := pusher(batch); err != nil {
				return pushedCnt, xerrors.Errorf("unable to push %d-elements batch: %w", len(batch), err)
			}
		}
		pushedCnt += int64(len(batch))
		batch = batch[:0]
	}

	return pushedCnt, splitter.Done()
}

func (r *PipeReader) itemFromTemplate(columnValues []byte) abstract.ChangeItem {
	item := r.template
	item.ColumnValues = []any{columnValues}
	return item
}

// Cancel can be called when all data was transferred.
// In such case, if PipeReader stucked on gpfdist.OpenPipe than pipe will never be opened
// because gpfdist have no data for that thread to transfer. Returning (0, nil) here is valid.
func (r *PipeReader) Close(ctx context.Context) (int64, error) {
	r.cancel()
	// Wait till Run finishes.
	select {
	case <-r.done:
		return r.pushedCnt, r.err
	case <-ctx.Done():
		return r.pushedCnt, xerrors.Errorf("context is done when waiting pipe reader closing")
	}
}

func (r *PipeReader) Status() (int64, error) {
	return r.pushedCnt, r.err
}

// Run should be called once per PipeReader life, it is not guaranteed that more calls will proceed.
func (r *PipeReader) Run(pusher abstract.Pusher) {
	defer close(r.done)
	r.err = r.runImpl(pusher)
}

func (r *PipeReader) runImpl(pusher abstract.Pusher) error {
	pipe, err := r.gpfdist.OpenPipe(r.ctx)
	if err != nil {
		return xerrors.Errorf("unable to open pipe: %w", err)
	}
	if pipe == nil {
		return nil // No data to transfer for that pipe (thread).
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
		r.pushedCnt += curRows
		errCh <- err
	}()
	select {
	case err := <-errCh:
		return err
	case <-r.ctx.Done():
		return xerrors.New("context is done before PipeReader worker")
	}
}

func NewPipeReader(gpfdist *gpfdistbin.Gpfdist, template abstract.ChangeItem) *PipeReader {
	ctx, cancel := context.WithCancel(context.Background())
	return &PipeReader{
		ctx:       ctx,
		cancel:    cancel,
		gpfdist:   gpfdist,
		template:  template,
		pushedCnt: 0,
		err:       nil,
		done:      make(chan struct{}),
	}
}
