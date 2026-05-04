package s3raw

import (
	"bytes"
	"context"
	"time"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher/fake_s3"
	"github.com/transferia/transferia/pkg/stats"
)

var (
	_ S3RawReader = (*FakeS3RawReader)(nil)
	_ ReaderAll   = (*FakeS3RawReader)(nil)
)

// FakeS3RawReader provides S3RawReader interface for FakeS3Client.
type FakeS3RawReader struct {
	file   *fake_s3.File
	reader *bytes.Reader
}

func (r *FakeS3RawReader) ReadAt(p []byte, off int64) (int, error) {
	if r == nil || r.reader == nil {
		return 0, xerrors.New("fake s3 reader is nil")
	}

	return r.reader.ReadAt(p, off)
}

func (r *FakeS3RawReader) Read(p []byte) (int, error) {
	if r == nil || r.reader == nil {
		return 0, xerrors.New("fake s3 reader is nil")
	}

	return r.reader.Read(p)
}

func (r *FakeS3RawReader) Close() error {
	return nil
}

func (r *FakeS3RawReader) Size() int64 {
	if r == nil || r.file == nil {
		return 0
	}
	return int64(len(r.file.Body))
}

func (r *FakeS3RawReader) LastModified() time.Time {
	if r == nil || r.file == nil {
		return time.Time{}
	}
	return r.file.LastModified
}

func (r *FakeS3RawReader) ReadAll(_ context.Context) ([]byte, error) {
	if r == nil || r.file == nil {
		return nil, xerrors.New("fake s3 reader is nil")
	}
	if len(r.file.Body) == 0 {
		return make([]byte, 0), nil
	}

	buf := make([]byte, len(r.file.Body))
	copy(buf, r.file.Body)
	return buf, nil
}

// builder

type FakeS3RawReaderBuilder struct {
	fakeS3ClientList    *fake_s3.FakeS3Client
	fakeS3ClientOneFile *fake_s3.FakeS3Client
}

var _ S3RawReaderBuilder = (*FakeS3RawReaderBuilder)(nil)

func (b *FakeS3RawReaderBuilder) BuildReader(_ context.Context, _ s3iface.S3API, _ string, filePath string, _ *stats.SourceStats) (S3RawReader, error) {
	file, err := b.fakeS3ClientList.PickFileForRead(filePath)
	if err != nil {
		return nil, err
	}
	return &FakeS3RawReader{
		file:   file,
		reader: bytes.NewReader(file.Body),
	}, nil
}

func (b *FakeS3RawReaderBuilder) BuildClient(_ *aws_session.Session) s3iface.S3API {
	return b.fakeS3ClientList
}

func NewFakeS3RawReaderBuilder(fakeS3ClientList *fake_s3.FakeS3Client, fakeS3ClientOneFile *fake_s3.FakeS3Client) *FakeS3RawReaderBuilder {
	return &FakeS3RawReaderBuilder{
		fakeS3ClientList:    fakeS3ClientList,
		fakeS3ClientOneFile: fakeS3ClientOneFile,
	}
}
