package util

import (
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
)

// Errors is a slice of error.
type Errors []error

// Error implements the error#Error method.
func (e Errors) Error() string {
	return ToString([]error(e))
}

// String implements the stringer#String method.
func (e Errors) String() string {
	return e.Error()
}

func (e Errors) Is(err error) bool {
	for _, currErr := range e {
		if xerrors.Is(currErr, err) {
			return true
		}
	}
	return false
}

func (e Errors) As(target any) bool {
	for _, currErr := range e {
		if xerrors.As(currErr, target) {
			return true
		}
	}
	return false
}

func (e Errors) Empty() bool {
	return len(e) == 0
}

// NewErrs returns a slice of error.
// Nil errors are thrown away
// If no errors are left in list, returns nil.
func NewErrs(err ...error) Errors {
	errs := yslices.Filter(err, func(err error) bool { return err != nil })
	if len(errs) == 0 {
		return nil
	}
	return errs
}

// AppendErr appends err to errors if it is not nil and returns the result.
// If err is nil, it is not appended.
func AppendErr(errors []error, err error) Errors {
	if err == nil {
		return errors
	}
	return append(errors, err)
}

// AppendErrs appends newErrs to errors and returns the result.
// If newErrs is empty, nothing is appended.
func AppendErrs(errors []error, newErrs []error) Errors {
	if len(newErrs) == 0 {
		return errors
	}
	for _, e := range newErrs {
		errors = AppendErr(errors, e)
	}
	if len(errors) == 0 {
		return nil
	}
	return errors
}

// ToString returns a string representation of errors. Any nil errors in the
// slice are skipped.
func ToString(errors []error) string {
	var out string
	for i, e := range errors {
		if e == nil {
			continue
		}
		if i != 0 {
			out += ", "
		}
		out += e.Error()
	}
	return out
}

// PrefixErrors prefixes each error within the supplied Errors slice with the
// string pfx.
func PrefixErrors(errs Errors, pfx string) Errors {
	var nerr Errors
	for _, err := range errs {
		nerr = append(nerr, fmt.Errorf("%s: %s", pfx, err))
	}
	return nerr
}

// UniqueErrors returns the unique errors from the supplied Errors slice. Errors
// are considered equal if they have equal stringified values.
func UniqueErrors(errs Errors) Errors {
	u := map[string]error{}
	for _, err := range errs {
		u[err.Error()] = err
	}

	var ne Errors
	for _, err := range u {
		ne = append(ne, err)
	}
	return ne
}

// MapErr is for applying mapping function to slice which may return error.
// All errors that occur during processing are stored in multi error object.
func MapErr[S ~[]T, T, M any](s S, fn func(T) (M, error)) ([]M, error) {
	var errs Errors
	result := yslices.Map(s, func(subs T) M {
		subt, err := fn(subs)
		errs = AppendErr(errs, err)
		return subt
	})
	if !errs.Empty() {
		return result, errs
	}
	return result, nil
}

// ForEachErr is for calling function for each element of slice which may return error.
// All errors that occur during processing are stored in multi error object.
func ForEachErr[S ~[]T, T any](s S, fn func(T) error) error {
	var errs Errors
	for _, t := range s {
		errs = AppendErr(errs, fn(t))
	}
	if !errs.Empty() {
		return errs
	}
	return nil
}
