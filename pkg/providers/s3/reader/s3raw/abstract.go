package s3raw

import (
	"context"
	"io"
	"math"
	"time"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/stats"
)

// TransportErrFmt is the printf layout for transport-layer errors returned at the s3raw boundary.
// Callers use xerrors.Errorf(TransportErrFmt, op, err); these values are not ReaderError until a format reader wraps them.
const TransportErrFmt = "s3raw transport error in %s: %w"

type S3RawReader interface {
	io.ReaderAt
	io.ReadCloser
	// Size returns logical byte length of the object (parquet-go openFile expects int64 Size()).
	Size() int64
	LastModified() time.Time
}

type S3RawReaderBuilder interface {
	BuildReader(ctx context.Context, client s3iface.S3API, bucket string, key string, metrics *stats.SourceStats) (S3RawReader, error)
	BuildClient(sess *aws_session.Session) s3iface.S3API
}

// ReaderAll returns whole file per one call. Used (if implemented) by some parsers.
// If not implemented, util.readAllByBlocks is used (which is for-loop calls of ReadAt).
type ReaderAll interface {
	ReadAll(ctx context.Context) ([]byte, error)
}

//---

type RealS3RawReaderBuilder struct{}

var _ S3RawReaderBuilder = (*RealS3RawReaderBuilder)(nil)

func (b *RealS3RawReaderBuilder) BuildReader(ctx context.Context, client s3iface.S3API, bucket string, key string, metrics *stats.SourceStats) (S3RawReader, error) {
	return NewS3RawReader(ctx, client, bucket, key, metrics)
}

func (b *RealS3RawReaderBuilder) BuildClient(sess *aws_session.Session) s3iface.S3API {
	return aws_s3.New(sess)
}

func NewRealS3RawReaderBuilder() *RealS3RawReaderBuilder {
	return &RealS3RawReaderBuilder{}
}

//---

var _ S3RawReader = (*StubS3RawReader)(nil)

// StubS3RawReader is a test helper with overridable methods.
type StubS3RawReader struct {
	fileSize uint64 // logical size; Size() returns int64 capped at math.MaxInt64
	ReadAtF  func(p []byte, off int64) (int, error)
	ReadF    func(p []byte) (int, error)
	CloseF   func() error
}

func (s *StubS3RawReader) ReadAt(p []byte, off int64) (int, error) {
	if s.ReadAtF != nil {
		return s.ReadAtF(p, off)
	}

	return 0, xerrors.New("not implemented")
}

func (s *StubS3RawReader) Close() error {
	if s.CloseF != nil {
		return s.CloseF()
	}
	return xerrors.New("not implemented")
}

func (s *StubS3RawReader) Read(p []byte) (int, error) {
	if s.ReadF != nil {
		return s.ReadF(p)
	}

	return 0, xerrors.New("not implemented")
}

func (s *StubS3RawReader) Size() int64 {
	if s.fileSize > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(s.fileSize)
}

func (s *StubS3RawReader) LastModified() time.Time {
	return time.Time{}
}

func NewStubS3RawReader(fileSize uint64) *StubS3RawReader {
	return &StubS3RawReader{
		fileSize: fileSize,
		ReadAtF:  nil,
		ReadF:    nil,
		CloseF:   nil,
	}
}
