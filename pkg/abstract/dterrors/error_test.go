package dterrors

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/core/xerrors"
)

func TestCheckForFatalError(t *testing.T) {
	CheckErrorWrapping(t, "default creation", IsFatal, NewFatalError)
	CheckErrorWrapping(t, "struct", IsFatal, func(err error) error {
		return baseFatalError{error: err}
	})
	CheckErrorWrapping(t, "pointer", IsFatal, func(err error) error {
		return &baseFatalError{error: err}
	})
}

func TestCheckForTableUploadError(t *testing.T) {
	CheckErrorWrapping(t, "default creation", IsTableUploadError, NewTableUploadError)
	CheckErrorWrapping(t, "struct", IsTableUploadError, func(err error) error {
		return TableUploadError{error: err}
	})
	CheckErrorWrapping(t, "pointer", IsTableUploadError, func(err error) error {
		return &TableUploadError{error: err}
	})
}

// replacement showcase
type ValueError int

func (v ValueError) Error() string { return "value error" }

type PointerError string

func (PointerError) Error() string { return "pointer error" }

func TestValueError(t *testing.T) {
	processWithCast := func(err error) (ValueError, error) {
		if val, ok := err.(ValueError); ok {
			return val, nil
		}
		return 0, xerrors.New("Can't cast error to ValueError")
	}
	processWithAs := func(err error) (ValueError, error) {
		var val ValueError
		if xerrors.As(err, &val) {
			return val, nil
		}
		return 0, xerrors.New("Can't cast error to ValueError")
	}

	someValueError := ValueError(3)
	res1, err1 := processWithAs(someValueError)
	require.NoError(t, err1)
	require.Equal(t, res1, someValueError)
	res2, err2 := processWithCast(someValueError)
	require.NoError(t, err2)
	require.Equal(t, res2, someValueError)

	wrappedSomeValue := xerrors.Errorf("val err: %w", someValueError)
	res3, err3 := processWithAs(wrappedSomeValue)
	require.NoError(t, err3)
	require.Equal(t, res3, someValueError)
	_, err4 := processWithCast(wrappedSomeValue)
	require.Error(t, err4, "explicit cast can't bring us to a result")
}

func TestPointerError(t *testing.T) {
	processWithCast := func(err error) (*PointerError, error) {
		if val, ok := err.(*PointerError); ok {
			return val, nil
		}
		return nil, xerrors.New("Can't cast error to PointerError")
	}
	processWithAs := func(err error) (*PointerError, error) {
		var val *PointerError
		if xerrors.As(err, &val) {
			return val, nil
		}
		return nil, xerrors.New("Can't cast error to PointerError")
	}

	someValueError := PointerError("abc")
	someValueErrorPtr := &someValueError
	res1, err1 := processWithAs(someValueErrorPtr)
	require.NoError(t, err1)
	require.Equal(t, res1, someValueErrorPtr)
	res2, err2 := processWithCast(someValueErrorPtr)
	require.NoError(t, err2)
	require.Equal(t, res2, someValueErrorPtr)

	wrappedSomeValue := xerrors.Errorf("val err: %w", someValueErrorPtr)
	res3, err3 := processWithAs(wrappedSomeValue)
	require.NoError(t, err3)
	require.Equal(t, res3, someValueErrorPtr)
	_, err4 := processWithCast(wrappedSomeValue)
	require.Error(t, err4, "explicit cast can't bring us to a result")
}
