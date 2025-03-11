package iter

import (
	"bufio"
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

var (
	_ Iter[string] = (*BlobIter)(nil)
)

// FromReadCloser returns an iterator producing lines from the given reader.
func FromReadCloser(r io.ReadCloser) *BlobIter {
	s := *bufio.NewScanner(r)
	// advance the line
	eof := s.Scan()

	return &BlobIter{
		err:     s.Err(),
		eof:     eof,
		reader:  r,
		scanner: s,
	}
}

type BlobIter struct {
	err     error
	eof     bool
	reader  io.ReadCloser
	scanner bufio.Scanner
}

func (it *BlobIter) Next() bool {
	return it.eof
}

// Value implements Iterator[T].Value by returning the next line from the
// reader.
func (it *BlobIter) Value() (string, error) {
	if it.err != nil {
		return "", xerrors.Errorf("scanner scan error: %w", it.err)
	}

	s := it.scanner.Text()
	it.eof = it.scanner.Scan()
	it.err = it.scanner.Err()

	return s, nil
}

func (it *BlobIter) Close() error {
	return it.reader.Close()
}
