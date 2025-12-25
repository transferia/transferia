package s3raw

import (
	"io"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type S3RawReader interface {
	io.ReaderAt
	io.ReadCloser
	Size() int64
	LastModified() time.Time
}

// ReaderAll returns whole file per one call. Used (if implemented) by some parsers.
// If not implemented, util.readAllByBlocks is used (which is for-loopo calls of ReadAt).
type ReaderAll interface {
	ReadAll() ([]byte, error)
}

//---

var _ S3RawReader = (*FakeS3RawReader)(nil)

type FakeS3RawReader struct {
	fileSize int64
	ReadAtF  func(p []byte, off int64) (int, error)
	ReadF    func(p []byte) (int, error)
	CloseF   func() error
}

func (f *FakeS3RawReader) ReadAt(p []byte, off int64) (int, error) {
	if f.ReadAtF != nil {
		return f.ReadAtF(p, off)
	}

	return 0, xerrors.New("not implemented")
}

func (f *FakeS3RawReader) Close() error {
	if f.CloseF != nil {
		return f.CloseF()
	}
	return xerrors.New("not implemented")
}

func (f *FakeS3RawReader) Read(p []byte) (int, error) {
	if f.ReadF != nil {
		return f.ReadF(p)
	}

	return 0, xerrors.New("not implemented")
}

func (f *FakeS3RawReader) Size() int64 {
	return f.fileSize
}

func (f *FakeS3RawReader) LastModified() time.Time {
	return time.Time{}
}

func NewFakeS3RawReader(fileSize int64) *FakeS3RawReader {
	return &FakeS3RawReader{
		fileSize: fileSize,
		ReadAtF:  nil,
		ReadF:    nil,
		CloseF:   nil,
	}
}
