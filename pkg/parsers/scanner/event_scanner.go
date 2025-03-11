package scanner

import (
	"github.com/transferria/transferria/pkg/abstract"
	"golang.org/x/xerrors"
)

type EventScanner interface {
	Scan() bool
	Event() ([]byte, error)
	Err() error
}

type Event struct {
	data []byte
	err  error
}

func EventCopyFromBytes(data []byte, err error) *Event {
	e := &Event{
		data: make([]byte, len(data)),
		err:  err,
	}

	copy(e.data, data)
	return e
}

func NewScanner(lineSplitter abstract.LfLineSplitter, data []byte) (EventScanner, error) {
	switch lineSplitter {
	case abstract.LfLineSplitterDoNotSplit:
		return NewDoNotSplitScanner(data), nil
	case abstract.LfLineSplitterNewLine:
		return NewLineBreakScanner(data), nil
	case abstract.LfLineSplitterProtoseq:
		return NewProtoseqScanner(data), nil
	}

	return nil, xerrors.Errorf("unknown scanner type '%s'", string(lineSplitter))
}
