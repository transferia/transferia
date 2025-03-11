package gpfdist

import (
	"bufio"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
	"github.com/transferia/transferia/pkg/util/slicesx"
	"golang.org/x/sync/errgroup"
)

type PipesWriter struct {
	gpfdist   *gpfdistbin.Gpfdist
	pushedCnt atomic.Int64
	pipes     []*os.File
	pipesMu   sync.RWMutex
}

// Stop returns number of rows, pushed to gpfdist's pipes.
func (w *PipesWriter) Stop() (int64, error) {
	w.pipesMu.Lock()
	defer w.pipesMu.Unlock()
	if w.pipes == nil {
		return 0, nil
	}
	var err error
	for _, pipe := range w.pipes {
		if curErr := pipe.Close(); curErr != nil {
			err = multierr.Append(err, xerrors.Errorf("unable to close pipe %s: %w", pipe.Name(), curErr))
		}
	}
	w.pipes = nil
	return w.pushedCnt.Load(), err
}

// Write pushes `input` by equal parts per each pipe.
func (w *PipesWriter) Write(input []string) error {
	w.pipesMu.RLock()
	defer w.pipesMu.RUnlock()
	if w.pipes == nil {
		return xerrors.New("pipes writer is closed")
	}

	chunks := slicesx.SplitToChunks(input, len(w.pipes))
	eg := errgroup.Group{}
	for i, pipe := range w.pipes {
		eg.Go(func() error {
			curRows, err := writeToPipe(pipe, chunks[i])
			w.pushedCnt.Add(curRows)
			return err
		})
	}
	return eg.Wait()
}

func writeToPipe(target io.Writer, input []string) (int64, error) {
	writer := bufio.NewWriter(target)
	var pushedCnt int64
	for _, line := range input {
		if _, err := writer.WriteString(line); err != nil {
			return pushedCnt, xerrors.Errorf("unable to write string: %w", err)
		}
		pushedCnt++
	}
	if err := writer.Flush(); err != nil {
		return pushedCnt, xerrors.Errorf("unable to flush: %w", err)
	}
	return pushedCnt, nil
}

func InitPipesWriter(gpfdist *gpfdistbin.Gpfdist) (*PipesWriter, error) {
	pipes, err := gpfdist.OpenPipes()
	if err != nil {
		return nil, xerrors.Errorf("unable to open pipes: %w", err)
	}
	return &PipesWriter{
		gpfdist:   gpfdist,
		pushedCnt: atomic.Int64{},
		pipes:     pipes,
		pipesMu:   sync.RWMutex{},
	}, nil
}
