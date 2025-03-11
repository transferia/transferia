package middlewares

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// NonRowSeparator separates non-row items and pushes each of them in a distinct call to Push (which will contain just one item)
func NonRowSeparator() func(abstract.Sinker) abstract.Sinker {
	return func(s abstract.Sinker) abstract.Sinker {
		return newNonRowSeparator(s)
	}
}

type nonRowSeparator struct {
	sink abstract.Sinker
}

func newNonRowSeparator(s abstract.Sinker) *nonRowSeparator {
	return &nonRowSeparator{
		sink: s,
	}
}

func (s *nonRowSeparator) Close() error {
	return s.sink.Close()
}

func (s *nonRowSeparator) Push(items []abstract.ChangeItem) error {
	iCurrentStart := 0
	iCurrentEnd := 0
	for i := range items {
		if items[i].IsRowEvent() {
			iCurrentEnd += 1
			continue
		}

		if iCurrentEnd > iCurrentStart {
			if err := s.sink.Push(items[iCurrentStart:iCurrentEnd]); err != nil {
				return xerrors.Errorf("failed to push items from %d to %d in batch: %w", iCurrentStart, iCurrentEnd, err)
			}
		}
		if err := s.sink.Push(items[i : i+1]); err != nil {
			return xerrors.Errorf("failed to push non-row item %d of kind %q in batch: %w", i, items[i].Kind, err)
		}
		iCurrentStart = i + 1
		iCurrentEnd = iCurrentStart
	}
	if iCurrentEnd > iCurrentStart {
		if err := s.sink.Push(items[iCurrentStart:iCurrentEnd]); err != nil {
			return xerrors.Errorf("failed to push items from %d to %d in batch: %w", iCurrentStart, iCurrentEnd, err)
		}
	}
	return nil
}
