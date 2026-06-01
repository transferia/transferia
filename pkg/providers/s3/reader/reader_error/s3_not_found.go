package reader_error

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"go.ytsaurus.tech/library/go/core/log"
)

const s3NotFoundCode = "NotFound"

// IsS3ObjectNotFound reports whether err is an AWS S3 missing-object response (NoSuchKey or NotFound).
func IsS3ObjectNotFound(err error) bool {
	for err != nil {
		var aerr awserr.Error
		if xerrors.As(err, &aerr) {
			switch aerr.Code() {
			case aws_s3.ErrCodeNoSuchKey, s3NotFoundCode:
				return true
			}
		}
		err = xerrors.Unwrap(err)
	}
	return false
}

// NewReaderErrorFromObjectOpen maps missing-object transport failures to ReaderErrorNoSuchFile.
func NewReaderErrorFromObjectOpen(op, file string, err error) ReaderError {
	if IsS3ObjectNotFound(err) {
		return NewReaderErrorNoSuchFile(op, file)
	}
	return NewReaderErrorTransport(op, file, err)
}

// ApplyNoSuchFileHandleMode applies configured missing-file policy to a ReaderError from Read.
// Continue logs a warning and returns nil; Fatal upgrades to ReaderErrorFatal; other errors pass through.
func ApplyNoSuchFileHandleMode(lgr log.Logger, mode s3_model.NoSuchFileHandleMode, err ReaderError) ReaderError {
	if err == nil {
		return nil
	}
	if _, ok := AsReaderErrorNoSuchFile(err); !ok {
		return err
	}
	if mode == "" {
		mode = s3_model.NoSuchFileHandleModeContinue
	}
	switch mode {
	case s3_model.NoSuchFileHandleModeFatal:
		return NewReaderErrorFatal("noSuchFile", xerrors.Errorf("%w", err))
	default:
		if lgr != nil {
			lgr.Warnf("S3 file not found, skipping and continuing: %v", err)
		}
		return nil
	}
}
