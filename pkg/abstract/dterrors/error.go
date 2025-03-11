package dterrors

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
)

// FatalError denotes an error which must stop the transfer completely, forbidding to restart it automatically
type FatalError interface {
	error
	IsFatal()
}

func IsFatal(err error) bool {
	var target FatalError
	return xerrors.As(err, &target)
}

// baseFatalError is a default implementation of FatalError interface
type baseFatalError struct{ error }

func (b baseFatalError) IsFatal() {}

func (b baseFatalError) Unwrap() error {
	return b.error
}

func NewFatalError(err error) error {
	if err == nil {
		return nil
	}
	return &baseFatalError{error: err}
}

//---

type TableUploadError struct{ error }

func (p TableUploadError) Unwrap() error {
	return p.error
}

func (p TableUploadError) Is(err error) bool {
	_, ok := err.(TableUploadError)
	return ok
}

func IsTableUploadError(err error) bool {
	return xerrors.Is(err, TableUploadError{error: nil})
}

func NewTableUploadError(err error) error {
	if err == nil {
		return nil
	}
	return &TableUploadError{err}
}

type RetriablePartUploadError struct {
	error
}

func (e *RetriablePartUploadError) Unwrap() error {
	return e.error
}

func (e *RetriablePartUploadError) Is(err error) bool {
	_, ok := err.(*RetriablePartUploadError)
	return ok
}

func NewRetriablePartUploadError(err error) *RetriablePartUploadError {
	return &RetriablePartUploadError{err}
}

func IsRetriablePartUploadError(err error) bool {
	return xerrors.Is(err, new(RetriablePartUploadError))
}

//---

type NonShardableError interface {
	error
	IsNonShardableError()
}

func IsNonShardableError(err error) bool {
	var target NonShardableError
	return xerrors.As(err, &target)
}

type nonShardableError struct{ error }

func (e nonShardableError) Unwrap() error {
	return e.error
}

func (e nonShardableError) IsNonShardableError() {}

func NewNonShardableError(err error) error {
	if err == nil {
		return nil
	}
	return &nonShardableError{error: err}
}
