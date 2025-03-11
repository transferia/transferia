package iter

import "github.com/transferria/transferria/library/go/core/xerrors"

type Iter[T any] interface {
	Next() bool
	Value() (T, error)
	Close() error
}

func Map[T any, R any](iter Iter[T], mapper func(t T) (R, error)) ([]R, error) {
	var res []R
	for iter.Next() {
		v, err := iter.Value()
		if err != nil {
			return nil, xerrors.Errorf("retrieve value from iterator: %w", err)
		}
		r, err := mapper(v)
		if err != nil {
			return nil, xerrors.Errorf("mapping value %v: %w", v, err)
		}
		res = append(res, r)
	}
	return res, nil
}
