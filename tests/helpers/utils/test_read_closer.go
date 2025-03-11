package utils

import (
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type TestReadCloser struct {
	stepsData [][]byte
}

func (r *TestReadCloser) Add(buf []byte) {
	r.stepsData = append(r.stepsData, buf)
}

func (r *TestReadCloser) Read(p []byte) (n int, err error) {
	if len(r.stepsData) == 0 {
		return 0, xerrors.Errorf("ReadCloser is empty")
	}

	var resultErr error = nil

	minLen := min(len(p), len(r.stepsData[0]))
	copy(p, r.stepsData[0][0:minLen])
	r.stepsData[0] = r.stepsData[0][minLen:]
	if len(r.stepsData[0]) == 0 {
		// roll to the new step
		r.stepsData = r.stepsData[1:]
		resultErr = io.EOF
	}

	return minLen, resultErr
}

func (r *TestReadCloser) Close() error {
	return nil
}

func NewTestReadCloser() *TestReadCloser {
	return &TestReadCloser{
		stepsData: nil,
	}
}
