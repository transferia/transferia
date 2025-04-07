package gpfdist

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/errgroup"
)

const (
	changeItemsBatchSize = 10000
	readFileBlockSize    = 25 * humanize.MiByte
	parseQueueCap        = 250 * humanize.MiByte / readFileBlockSize
)

type AsyncSplitter struct {
	quotesCnt int
	buffer    []byte
	ResCh     chan [][]byte
	DoneCh    chan error
}

func InitAsyncSplitter(input <-chan []byte) *AsyncSplitter {
	s := &AsyncSplitter{
		quotesCnt: 0,
		buffer:    nil,
		ResCh:     make(chan [][]byte, parseQueueCap),
		DoneCh:    make(chan error, 1),
	}
	go func() {
		defer close(s.ResCh)
		defer close(s.DoneCh)
		for bytes := range input {
			if res := s.doPart(bytes); len(res) > 0 {
				s.ResCh <- res
			}
		}
		if len(s.buffer) > 0 {
			s.DoneCh <- xerrors.New("buffer is not empty")
		}
	}()
	return s
}

func (s *AsyncSplitter) doPart(bytes []byte) [][]byte {
	res := make([][]byte, 0, 100_000)
	lineStartIndex := 0
	for i := range bytes {
		if bytes[i] == '"' {
			s.quotesCnt++
			continue
		}
		if bytes[i] != '\n' || s.quotesCnt%2 != 0 {
			continue
		}
		// Found '\n' which is not escaped by '"', flush line.
		var curRes []byte
		if len(s.buffer) > 0 {
			curRes = append(s.buffer, bytes[lineStartIndex:i+1]...)
		} else {
			curRes = bytes[lineStartIndex : i+1]
		}
		if len(curRes) > 0 {
			res = append(res, curRes)
		}
		s.quotesCnt = 0
		s.buffer = nil
		lineStartIndex = i + 1
	}
	s.buffer = append(s.buffer, bytes[lineStartIndex:]...)
	return res
}

type PipeReader struct {
	ctx       context.Context
	gpfdist   *gpfdistbin.Gpfdist
	template  abstract.ChangeItem
	batchSize int
	pushedCnt atomic.Int64
	errCh     chan error
}

func (r *PipeReader) readFromPipe(file *os.File, pusher abstract.Pusher) (int64, error) {
	pushedCnt := int64(0)
	parseQueue := make(chan []byte, parseQueueCap)
	splitter := InitAsyncSplitter(parseQueue)

	eg := errgroup.Group{}
	eg.Go(func() error {
		batch := make([]abstract.ChangeItem, 0, changeItemsBatchSize)
		for lines := range splitter.ResCh {
			for _, line := range lines {
				batch = append(batch, r.itemFromTemplate(line))
				if len(batch) < changeItemsBatchSize {
					continue
				}
				if err := pusher(batch); err != nil {
					return xerrors.Errorf("unable to push %d-elements batch: %w", len(batch), err)
				}
				pushedCnt += int64(len(batch))
				batch = make([]abstract.ChangeItem, 0, changeItemsBatchSize)
			}
		}
		if len(batch) > 0 {
			if err := pusher(batch); err != nil {
				return xerrors.Errorf("unable to push last %d-elements batch: %w", len(batch), err)
			}
			pushedCnt += int64(len(batch))
		}
		return nil
	})

	eg.Go(func() error {
		defer close(parseQueue)
		for {
			b := make([]byte, readFileBlockSize)
			n, err := io.ReadAtLeast(file, b, len(b))
			if err == io.EOF {
				break
			}
			if err != nil && err != io.ErrUnexpectedEOF {
				return xerrors.Errorf("unable to read file: %w", err)
			}
			parseQueue <- b[:n]
		}
		return nil
	})

	err := eg.Wait()
	return pushedCnt, err
}

func (r *PipeReader) itemFromTemplate(columnValues []byte) abstract.ChangeItem {
	item := r.template
	item.ColumnValues = []any{columnValues}
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
