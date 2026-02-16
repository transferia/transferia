package writer

import (
	"compress/gzip"
	"compress/zlib"
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
)

type Writer struct {
	io.WriteCloser
	wrappedCloser []io.Closer
}

func (w *Writer) Close() error {
	// First close the WriteCloser (e.g., gzip.Writer) to flush all data
	if err := w.WriteCloser.Close(); err != nil {
		return xerrors.Errorf("failed to close wrapped writer: %w", err)
	}

	// Then close the underlying writer (e.g., pipeWriter)
	for _, closer := range w.wrappedCloser {
		if err := closer.Close(); err != nil {
			return xerrors.Errorf("failed to close wrapped closer: %w", err)
		}
	}

	return nil
}

func (w *Writer) Write(p []byte) (int, error) {
	return w.WriteCloser.Write(p)
}

func withCloser(w io.WriteCloser, closer io.Closer) io.WriteCloser {
	return &Writer{
		WriteCloser:   w,
		wrappedCloser: []io.Closer{closer},
	}
}

func NewWriter(outputEncoding s3_provider.Encoding, rawWriter io.WriteCloser) io.WriteCloser {
	switch outputEncoding {
	case s3_provider.GzipEncoding:
		return withCloser(gzip.NewWriter(rawWriter), rawWriter)
	case s3_provider.ZlibEncoding:
		return withCloser(zlib.NewWriter(rawWriter), rawWriter)
	default:
		// without compression
		return &Writer{
			WriteCloser:   rawWriter,
			wrappedCloser: []io.Closer{rawWriter},
		}
	}
}
