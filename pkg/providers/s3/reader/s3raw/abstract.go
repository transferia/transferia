//go:build !disable_s3_provider

package s3raw

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type AbstractS3RawReader interface {
	ReadAt(p []byte, off int64) (int, error)
	Size() int64
	LastModified() time.Time
}

//---

type FakeS3RawReader struct {
	fileSize int64
}

func (f *FakeS3RawReader) ReadAt(p []byte, off int64) (int, error) {
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
	}
}
