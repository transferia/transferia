package gpfdist

import (
	"bufio"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/transferia/transferia/library/go/core/xerrors"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
)

type PipeWriter struct {
	gpfdist   *gpfdistbin.Gpfdist
	pushedCnt atomic.Int64
	pipe      *os.File
	pipeMu    sync.RWMutex
}

// Stop returns number of rows, pushed to gpfdist's pipe.
func (w *PipeWriter) Stop() (int64, error) {
	w.pipeMu.Lock()
	defer w.pipeMu.Unlock()
	if w.pipe == nil {
		return 0, nil
	}
	err := w.pipe.Close()
	w.pipe = nil
	return w.pushedCnt.Load(), err
}

// Write pushes `input` by equal parts per each pipe.
func (w *PipeWriter) Write(input []string) error {
	w.pipeMu.RLock()
	defer w.pipeMu.RUnlock()
	if w.pipe == nil {
		return xerrors.New("pipe writer is closed")
	}
	curRows, err := writeToPipe(w.pipe, input)
	w.pushedCnt.Add(curRows)
	return err
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

func InitPipeWriter(gpfdist *gpfdistbin.Gpfdist) (*PipeWriter, error) {
	pipe, err := gpfdist.OpenPipe()
	if err != nil {
		return nil, xerrors.Errorf("unable to open pipe: %w", err)
	}
	return &PipeWriter{
		gpfdist:   gpfdist,
		pushedCnt: atomic.Int64{},
		pipe:      pipe,
		pipeMu:    sync.RWMutex{},
	}, nil
}
