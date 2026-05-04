package reader_error

import (
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
)

// SchemaSampleErrorMayFallThroughToNextFile reports whether schema inference may try the next S3 object
// after a failed sample (used when UnparsedPolicyContinue is set on the source).
func SchemaSampleErrorMayFallThroughToNextFile(policy s3_model.UnparsedPolicy, err ReaderError) bool {
	if err == nil || policy != s3_model.UnparsedPolicyContinue {
		return false
	}
	switch err.(type) {
	case ReaderErrorTransport, ReaderErrorSink, ReaderErrorFatal, ReaderErrorConfig, ReaderErrorNoFiles:
		return false
	case ReaderErrorData:
		return true
	default:
		return false
	}
}
