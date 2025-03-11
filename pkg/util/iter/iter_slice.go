package iter

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
)

type sliceIter[T any] struct {
	items []T
	i     int
}

func (s *sliceIter[T]) Next() bool {
	return s.i < len(s.items)
}

func (s *sliceIter[T]) Value() (T, error) {
	var item T
	if s.i < len(s.items) {
		item = s.items[s.i]
		s.i++
		return item, nil
	}
	return item, nil
}

func (s *sliceIter[T]) Close() error {
	return nil
}

func ToSlice[T any](iter Iter[T]) ([]T, error) {
	if iter == nil {
		return nil, xerrors.New("nil iterator")
	}

	var s []T
	for iter.Next() {
		item, err := iter.Value()
		if err != nil {
			return nil, xerrors.Errorf("unable to fetch value: %w", err)
		}
		s = append(s, item)
	}
	_ = iter.Close()
	return s, nil
}

func FromSlice[T any](items ...T) Iter[T] {
	return &sliceIter[T]{
		items: items,
		i:     0,
	}
}
