package reader_error

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

func TestIsS3ObjectNotFound(t *testing.T) {
	t.Parallel()
	require.True(t, IsS3ObjectNotFound(awserr.New(aws_s3.ErrCodeNoSuchKey, "missing", nil)))
	require.True(t, IsS3ObjectNotFound(awserr.New(s3NotFoundCode, "missing", nil)))
	require.True(t, IsS3ObjectNotFound(xerrors.Errorf("wrap: %w", awserr.New(aws_s3.ErrCodeNoSuchKey, "missing", nil))))
	require.False(t, IsS3ObjectNotFound(xerrors.New("other")))
}

func TestNewReaderErrorFromObjectOpen(t *testing.T) {
	t.Parallel()
	missing := awserr.New(aws_s3.ErrCodeNoSuchKey, "missing", nil)
	err := NewReaderErrorFromObjectOpen("op", "k", missing)
	_, ok := AsReaderErrorNoSuchFile(err)
	require.True(t, ok)

	other := xerrors.New("network")
	err = NewReaderErrorFromObjectOpen("op", "k", other)
	_, ok = err.(ReaderErrorTransport)
	require.True(t, ok)
}
