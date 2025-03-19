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
	"golang.org/x/sync/errgroup"
)

type PipesReader struct {
	ctx       context.Context
	gpfdist   *gpfdistbin.Gpfdist
	template  abstract.ChangeItem
	batchSize int
	pushedCnt atomic.Int64
	errCh     chan error
}

func (r *PipesReader) readFromPipe(reader io.Reader, pusher abstract.Pusher) (int64, error) {
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

func (r *PipesReader) itemFromTemplate(columnValues []any) abstract.ChangeItem {
	item := r.template
	item.ColumnValues = columnValues
	return item
}

func (r *PipesReader) Stop(timeout time.Duration) (int64, error) {
	var cancel context.CancelFunc
	r.ctx, cancel = context.WithTimeout(r.ctx, timeout)
	defer cancel()
	err := <-r.errCh
	return r.pushedCnt.Load(), err
}

// Run should be called once per PipesReader life, it is not guaranteed that more calls will proceed.
func (r *PipesReader) Run(pusher abstract.Pusher) {
	r.errCh <- r.runImpl(pusher)
}

func (r *PipesReader) runImpl(pusher abstract.Pusher) error {
	pipes, err := r.gpfdist.OpenPipes()
	if err != nil {
		return xerrors.Errorf("unable to open pipes: %w", err)
	}
	defer func() {
		for _, pipe := range pipes {
			if err := pipe.Close(); err != nil {
				logger.Log.Error(fmt.Sprintf("Unable to close pipe %s", pipe.Name()), log.Error(err))
			}
		}
	}()
	eg := errgroup.Group{}
	for _, pipe := range pipes {
		eg.Go(func() error {
			curRows, err := r.readFromPipe(pipe, pusher)
			r.pushedCnt.Add(curRows)
			return err
		})
	}
	egErrCh := make(chan error, 1)
	go func() { egErrCh <- eg.Wait() }()
	select {
	case err := <-egErrCh:
		return err
	case <-r.ctx.Done():
		return xerrors.New("context is done before PipeReader workers")
	}
}

func NewPipesReader(gpfdist *gpfdistbin.Gpfdist, template abstract.ChangeItem, batchSize int) *PipesReader {
	return &PipesReader{
		ctx:       context.Background(),
		gpfdist:   gpfdist,
		template:  template,
		batchSize: batchSize,
		pushedCnt: atomic.Int64{},
		errCh:     make(chan error, 1),
	}
}
