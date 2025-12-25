package csv

import (
	"bufio"
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

// This is csv-specific line-splitter, who validates quotes
// pkg/util contains common line-splitter (LineSplitter), which is based on this splitter, but without quotes checking

type splitterState int

const (
	// Normal state, the default one upon initialization
	outsideQuote = splitterState(iota)

	// We are inside double quotes and did not find the closing quote yet
	quoteOpen

	// We are inside double quotes, found the closing quote and we are yet to
	// decide whether it is the closing double quote or an escape sequence
	// (i.e. "", two double quotes inside double quotes)
	closingQuote
)

// Splitter splits CSV formatted data into separate entries, taking into
// account line breaks, quoting and escaping, and writes those entries into the
// given writer.
type Splitter struct {
	reader bufio.Reader
	writer io.Writer
	state  splitterState
}

// ConsumeRow reads one CSV entry from the input and writes it into the writer.
func (s *Splitter) ConsumeRow() error {
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
		s.updateState(slice)

		switch readErr {
		case nil:
			if s.state == outsideQuote {
				return nil
			}
		case bufio.ErrBufferFull:
		case io.EOF:
			return io.EOF
		default:
			return xerrors.Errorf("read: %w", readErr)
		}
	}
}

func (s *Splitter) updateState(slice []byte) {
	for _, char := range slice {
		switch s.state {
		case outsideQuote:
			if char == '"' {
				s.state = quoteOpen
			}
		case quoteOpen:
			if char == '"' {
				s.state = closingQuote
			}
		case closingQuote:
			if char == '"' {
				s.state = quoteOpen
			} else {
				s.state = outsideQuote
			}
		}
	}
}

func NewSplitter(reader io.Reader, writer io.Writer) *Splitter {
	return &Splitter{
		reader: *bufio.NewReader(reader),
		writer: writer,
		state:  outsideQuote,
	}
}

func NewSplitterSize(reader io.Reader, writer io.Writer, size int) *Splitter {
	return &Splitter{
		reader: *bufio.NewReaderSize(reader, size),
		writer: writer,
		state:  outsideQuote,
	}
}
