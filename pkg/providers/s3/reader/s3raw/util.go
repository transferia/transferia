package s3raw

import (
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

func calcRange(p []byte, off int64, totalSize int64) (int64, int64, error) {
	var err error
	if off < 0 {
		return 0, 0, xerrors.New("negative offset not allowed")
	}

	if totalSize <= 0 {
		return 0, 0, xerrors.New("unable to read form object with no size")
	}

	start := off
	end := off + int64(len(p)) - 1
	if end >= totalSize {
		// Clamp down the requested range.
		end = totalSize - 1
		err = io.EOF

		if end < start {
			// this occurs when offset is bigger than the total size of the object
			return 0, 0, xerrors.New("offset outside of possible range")
		}
		if end-start > int64(len(p)) {
			// should never occur
			return 0, 0, xerrors.New("covered range is bigger than full object size")
		}
	}

	return start, end, err
}
