package iter

import "github.com/transferria/transferria/library/go/core/xerrors"

type MapIter[T any, R any] struct {
	It     Iter[T]
	Mapper func(t T) (R, error)
}

func (d *MapIter[T, R]) Next() bool {
	return d.It.Next()
}

func (d *MapIter[T, R]) Value() (R, error) {
	var res R
	t, err := d.It.Value()
	if err != nil {
		return res, xerrors.Errorf("unable to extract value: %w", err)
	}
	return d.Mapper(t)
}

func (d *MapIter[T, R]) Close() error {
	return d.It.Close()
}
