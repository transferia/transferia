package dterrors

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	errors "golang.org/x/xerrors"
)

// CheckErrorWrapping checks that your custom error wrapper will not be damaged
// by other wrappers and vice vera.
// Use this function to run sub-test for your custom errors like this:
//
//	CheckErrorWrapping(t, "default creation", IsFatal, NewFatalError)
//
// Parameters:
//
//	comment:   comment for your sub-test
//	isOpaque:  if true, error removes type information about underlying error or does not using underlying error at all
//	predicate: function determines is some error matches your error, e.g. IsFatal(err)
//	newError:  error constructor (wrapper), at least check default error constructor
func CheckErrorWrapping(t *testing.T, comment string,
	predicate func(error) bool,
	newError func(error) error) {
	checkErrorWrappingGeneral(t, comment, false, predicate, newError)
}

// CheckOpaqueErrorWrapping is the same as CheckErrorWrapping, but for opaque errors
// can be used for checking new errors as well
func CheckOpaqueErrorWrapping(t *testing.T, comment string,
	predicate func(error) bool,
	newError func(error) error) {
	checkErrorWrappingGeneral(t, comment, true, predicate, newError)
}

func checkErrorWrappingGeneral(t *testing.T, comment string, isOpaque bool,
	predicate func(error) bool,
	newError func(error) error) {
	t.Run(comment, func(t *testing.T) {
		t.Parallel()
		simpleError := xerrors.New("mock error")
		wrappedSimpleError := xerrors.Errorf("wrapped mock error: %w", simpleError)
		relevantError := newError(simpleError)
		wrappedRelevantError := xerrors.Errorf("wrapped fatal error: %w", relevantError)
		wrappedRelevantError2 := xerrors.Errorf("wrapped fatal error [2]: %w", relevantError)
		opaqueError := xerrors.Errorf("opaque: %v", relevantError)
		opaqueError2 := errors.Opaque(relevantError)

		require.False(t, predicate(simpleError), "irrelevant errors should not satisfy predicate")
		require.False(t, predicate(wrappedSimpleError), "wrapped irrelevant errors should not satisfy predicate")
		require.False(t, predicate(opaqueError), "opaque wrapped error should not satisfy predicate")  // NOTE!
		require.False(t, predicate(opaqueError2), "opaque wrapped error should not satisfy predicate") // NOTE!

		require.True(t, predicate(relevantError), "relevant error should satisfy predicate")
		require.True(t, predicate(wrappedRelevantError), "wrapped relevant error should satisfy predicate")
		require.True(t, predicate(wrappedRelevantError2), "wrapped relevant error should satisfy predicate")

		notOpaqueErrorFatal := newError(NewFatalError(simpleError))
		notOpaqueErrorTable := newError(NewTableUploadError(simpleError))
		if isOpaque {
			require.False(t, IsFatal(notOpaqueErrorFatal), "opaque error should corrupt FatalError")
			require.False(t, IsTableUploadError(notOpaqueErrorTable), "opaque error should corrupt TableUploadError")
		} else {
			require.True(t, IsFatal(notOpaqueErrorFatal), "error should not corrupt FatalError")
			require.True(t, IsTableUploadError(notOpaqueErrorTable), "error should not corrupt TableUploadError")
		}
	})
}
