package reader_error

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// AsReaderErrorData returns (ReaderErrorData, true) if err unwraps to ReaderErrorData or *ReaderErrorData.
// Transport/sink/fatal wrappers must not match — those are not data-layer errors for UnparsedPolicy routing.
//
// errors.As cannot match [ReaderErrorData] values stored in an error interface (only *ReaderErrorData),
// so this walks the unwrap chain and type-switches explicitly.
func AsReaderErrorData(err error) (ReaderErrorData, bool) {
	var none ReaderErrorData
	for err != nil {
		switch e := err.(type) {
		case ReaderErrorData:
			return e, true
		case *ReaderErrorData:
			if e != nil {
				return *e, true
			}
		}
		err = xerrors.Unwrap(err)
	}

	return none, false
}

// AsReaderErrorNoFiles returns (ReaderErrorNoFiles, true) if err unwraps to ReaderErrorNoFiles.
func AsReaderErrorNoFiles(err error) (ReaderErrorNoFiles, bool) {
	var none ReaderErrorNoFiles
	for err != nil {
		switch e := err.(type) {
		case ReaderErrorNoFiles:
			return e, true
		case *ReaderErrorNoFiles:
			if e != nil {
				return *e, true
			}
		}
		err = xerrors.Unwrap(err)
	}
	return none, false
}

// ReaderErrorFromPush maps a sink Push failure to ReaderErrorSink (retriable) or ReaderErrorFatal.
func ReaderErrorFromPush(op, filePath string, err error) ReaderError {
	if err == nil {
		return nil
	}
	if abstract.IsFatal(err) {
		return NewReaderErrorFatal(op, err)
	}
	return NewReaderErrorSink(op, filePath, err)
}

func extendOp(op, current string) string {
	if current == "" {
		return op
	}
	return op + ":" + current
}

// wrapReaderErrorOpExtend prefixes stepOp onto the inner operation path for each concrete ReaderError kind:
// ReaderErrorTransport, ReaderErrorData, ReaderErrorConfig, ReaderErrorSink, ReaderErrorFatal, ReaderErrorNoFiles.
func wrapReaderErrorOpExtend(stepOp string, err ReaderError) ReaderError {
	if err == nil {
		return nil
	}

	switch e := err.(type) {
	case ReaderErrorTransport:
		e.op = extendOp(stepOp, e.op)
		return e

	case ReaderErrorData:
		e.Op = extendOp(stepOp, e.Op)
		return e

	case ReaderErrorConfig:
		e.op = extendOp(stepOp, e.op)
		return e

	case ReaderErrorSink:
		e.op = extendOp(stepOp, e.op)
		return e

	case ReaderErrorFatal:
		e.op = extendOp(stepOp, e.op)
		return e

	case ReaderErrorNoFiles:
		e.op = extendOp(stepOp, e.op)
		return e

	default:
		return NewReaderErrorConfig("impossible situation", err)
	}
}

func WrapReaderError(op string, err ReaderError) ReaderError {
	return wrapReaderErrorOpExtend(op, err)
}

// WrapContractorReadStep is used by ReaderContractor to attach the contractor step to reader-layer errors.
// It handles the same concrete ReaderError kinds as WrapReaderError.
func WrapContractorReadStep(stepOp string, err ReaderError) ReaderError {
	return wrapReaderErrorOpExtend(stepOp, err)
}
