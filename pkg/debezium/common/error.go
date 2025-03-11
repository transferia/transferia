package common

import "github.com/transferia/transferia/library/go/core/xerrors"

type UnknownTypeError interface {
	error
	IsUnknownTypeError()
}

func IsUnknownTypeError(err error) bool {
	var target UnknownTypeError
	return xerrors.As(err, &target)
}

//---

type baseUnknownTypeError struct{ error }

func (b baseUnknownTypeError) IsUnknownTypeError() {}

func (b baseUnknownTypeError) Unwrap() error {
	return b.error
}

func NewUnknownTypeError(err error) error {
	if err == nil {
		return nil
	}
	return &baseUnknownTypeError{error: err}
}
