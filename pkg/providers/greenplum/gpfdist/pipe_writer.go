package gpfdist

import (
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

func (w *PipeWriter) Write(input [][]byte) error {
	w.pipeMu.RLock()
	defer w.pipeMu.RUnlock()
	if w.pipe == nil {
		return xerrors.New("pipe writer is closed")
	}
	for _, line := range input {
		if _, err := w.pipe.Write(line); err != nil {
			return xerrors.Errorf("unable to write to %s: %w", w.pipe.Name(), err)
		}
		w.pushedCnt.Add(1)
	}
	return nil
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
