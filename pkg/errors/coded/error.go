package coded

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
)

// CodedError is an error with an attached code
type CodedError interface {
	error
	// Code is an uniq identifier of some specific error that can be referred from documentation or code / sdk.
	Code() Code
}

type codedImpl struct {
	error
	code Code
}

// CategorizedErrorf produces a xerrors-wrapped error with a given assigned category
func Errorf(code Code, format string, a ...any) CodedError {
	return &codedImpl{
		error: xerrors.Errorf(format, a...),
		code:  code,
	}
}

func (i *codedImpl) Unwrap() error {
	return i.error
}

func (i *codedImpl) Code() Code {
	return i.code
}
