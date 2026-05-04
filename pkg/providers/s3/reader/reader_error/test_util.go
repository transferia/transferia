package reader_error

import "github.com/transferia/transferia/library/go/core/xerrors"

// ForTestsClassifiedReaderLayer reports whether err is or wraps any reader_error.ReaderError implementation
// (transport, data, sink, config, fatal). Used by tests that assert "classified" errors without
// conflating data-layer AsReaderErrorData with transport/sink.
func ForTestsClassifiedReaderLayer(err error) bool {
	for err != nil {
		switch err.(type) {
		case ReaderErrorData, *ReaderErrorData,
			ReaderErrorTransport, *ReaderErrorTransport,
			ReaderErrorSink, *ReaderErrorSink,
			ReaderErrorFatal, *ReaderErrorFatal,
			ReaderErrorConfig, *ReaderErrorConfig:
			return true
		}
		err = xerrors.Unwrap(err)
	}
	return false
}
