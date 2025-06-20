//go:build !disable_s3_provider

package s3raw

import (
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

// calcRange calculates range ([begin, end], inclusive) to iterate over p starting with offset.
// It is guaranteed that end < totalSize.
func calcRange(bufferSize, offset, totalSize int64) (int64, int64, error) {
	if offset < 0 {
		return 0, 0, xerrors.New("offset is negative")
	}
	if totalSize <= 0 {
		return 0, 0, xerrors.New("totalSize is negative or zero")
	}
	if offset >= totalSize {
		return 0, 0, xerrors.New("offset is bigger than totalSize")
	}
	if bufferSize == 0 {
		return 0, 0, xerrors.New("size of p is zero")
	}

	start := offset
	end := offset + int64(bufferSize) - 1
	if end < totalSize {
		return start, end, nil
	}
	return start, totalSize - 1, io.EOF
}
