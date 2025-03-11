package util

import (
	"bufio"
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

// common line splitter, based on csv-specific line splitter from pkg/csv

// LineSplitter splits text, taking into account line breaks, and writes those entries into the given writer.
type LineSplitter struct {
	reader bufio.Reader
	writer io.Writer
}

// ConsumeRow reads one line from the input and writes it into the writer.
func (s *LineSplitter) ConsumeRow() error {
	for {
		// 'slice' here - not necessarily whole line (till newline)
		// Reader can be buffered, and reading long line will be part-by-part
		// When reading part-by-part - reader returns 'bufio.ErrBufferFull' error
		// Cycle 'for' here needed - to provide this part-by-part behaviour

		slice, readErr := s.reader.ReadSlice('\n')
		_, writeErr := s.writer.Write(slice)
		if writeErr != nil {
			return xerrors.Errorf("write: %w", writeErr)
		}

		switch readErr {
		case nil:
			return nil
		case bufio.ErrBufferFull:
		case io.EOF:
			return io.EOF
		default:
			return xerrors.Errorf("read: %w", readErr)
		}
	}
}

func NewLineSplitter(reader io.Reader, writer io.Writer) *LineSplitter {
	return &LineSplitter{
		reader: *bufio.NewReader(reader),
		writer: writer,
	}
}

func NewLineSplitterSize(reader io.Reader, writer io.Writer, size int) *LineSplitter {
	return &LineSplitter{
		reader: *bufio.NewReaderSize(reader, size),
		writer: writer,
	}
}
