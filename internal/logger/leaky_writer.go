package logger

import (
	"fmt"

	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/util/size"
)

const (
	leakageInfoMsg = "%v messages of total size %v were leaked"
)

type LeakyWriter struct {
	writer Writer

	maxSize int

	successSizeHist metrics.Histogram
	leakedSizeHist  metrics.Histogram

	leakedCount       int
	leakedCounter     metrics.Counter
	leakedSize        int
	leakedSizeCounter metrics.Counter
}

func NewLeakyWriter(writer Writer, registry metrics.Registry, maxSize int) *LeakyWriter {
	return &LeakyWriter{
		writer:            writer,
		maxSize:           maxSize,
		successSizeHist:   registry.Histogram("logger.success_size_hist", size.DefaultBuckets()),
		leakedSizeHist:    registry.Histogram("logger.leaked_size_hist", size.DefaultBuckets()),
		leakedCount:       0,
		leakedCounter:     registry.Counter("logger.leaked_count"),
		leakedSize:        0,
		leakedSizeCounter: registry.Counter("logger.leaked_size"),
	}
}

func (w *LeakyWriter) Write(p []byte) (int, error) {
	if !w.checkIfCanWrite(p) {
		return len(p), nil
	}

	if w.leakedCount > 0 {
		msg := fmt.Sprintf(leakageInfoMsg, w.leakedCount, w.leakedSize)
		w.leakedCount = 0
		w.leakedSize = 0
		if _, err := w.writer.Write([]byte(msg)); err != nil {
			return 0, xerrors.Errorf("unable to write leakage info: %w", err)
		}
	}

	if !w.checkIfCanWrite(p) {
		return len(p), nil
	}

	return w.writer.Write(p)
}

func (w *LeakyWriter) checkIfCanWrite(p []byte) bool {
	if w.canWrite(p) {
		w.successSizeHist.RecordValue(float64(len(p)))
		return true
	}

	w.leakedCount++
	w.leakedCounter.Inc()
	w.leakedSize += len(p)
	w.leakedSizeCounter.Add(int64(len(p)))
	w.leakedSizeHist.RecordValue(float64(len(p)))
	return false
}

func (w *LeakyWriter) canWrite(p []byte) bool {
	if w.maxSize > 0 && len(p) > w.maxSize {
		return false
	}
	return w.writer.CanWrite()
}
