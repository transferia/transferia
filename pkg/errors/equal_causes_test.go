package errors

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/castx"
)

func TestEqualCausesDifferentFiles(t *testing.T) {
	_, a := castx.ToByteSliceE(time.Unix(0, 0))
	require.Error(t, a)
	b := xerrors.Errorf("sample error")

	require.False(t, EqualCauses(a, b))
	require.False(t, EqualCauses(b, a))
}

func TestEqualCausesSameFileEqual(t *testing.T) {
	_, a := castx.ToByteSliceE(time.Unix(0, 0))
	require.Error(t, a)
	_, b := castx.ToByteSliceE(time.Unix(1, 0))
	require.Error(t, b)

	require.True(t, EqualCauses(a, b))
	require.True(t, EqualCauses(b, a))
}

func TestEqualCausesSameFileUnequal(t *testing.T) {
	a := multipleErrorsGenerator(2)
	b := multipleErrorsGenerator(3)

	require.False(t, EqualCauses(a, b))
	require.False(t, EqualCauses(b, a))
}

func multipleErrorsGenerator(definer int) error {
	switch {
	case definer%2 == 0:
		return xerrors.New("2 definer error")
	case definer%3 == 0:
		return xerrors.Errorf("3 definer error: %w", sentinelError)
	case definer%11 == 0:
		return xerrors.Errorf("11 definer error: %w", sentinelError)
	case definer%13 == 0:
		return xerrors.Errorf("13 definer error: %w", instantiatedError)
	case definer%101 == 0:
		return errors.New("101 definer error")
	case definer%103 == 0:
		return errors.New("103 definer error")
	default:
		return xerrors.Errorf("default definer error: %w", instantiatedError)
	}
}

var sentinelError = xerrors.NewSentinel("sentinel error")
var instantiatedError = xerrors.New("instantiated error")

func TestEqualCausesSameFileUnequalSentinel(t *testing.T) {
	a := multipleErrorsGenerator(3)
	b := multipleErrorsGenerator(11)

	require.False(t, EqualCauses(a, b))
	require.False(t, EqualCauses(b, a))
}

func TestEqualCausesSameFileEqualInstantiated(t *testing.T) {
	a := multipleErrorsGenerator(13)
	b := multipleErrorsGenerator(17)

	require.True(t, EqualCauses(a, b))
	require.True(t, EqualCauses(b, a))
}

func TestEqualCausesOneNil(t *testing.T) {
	a := multipleErrorsGenerator(3)
	var b error = nil

	require.False(t, EqualCauses(a, b))
	require.False(t, EqualCauses(b, a))
}

func TestEqualCausesBothNil(t *testing.T) {
	var a error = nil
	var b error = nil

	require.False(t, EqualCauses(a, b))
	require.False(t, EqualCauses(b, a))
}

func TestEqualCausesGolangErrorsUnequal(t *testing.T) {
	a := multipleErrorsGenerator(101)
	b := multipleErrorsGenerator(101)

	require.False(t, EqualCauses(a, b))
	require.False(t, EqualCauses(b, a))
}

func TestEqualCausesGolangErrorsEqualWithXErrors(t *testing.T) {
	a := wrapErrorInXerrors(multipleErrorsGenerator(101))
	b := wrapErrorInXerrors(multipleErrorsGenerator(101))

	require.True(t, EqualCauses(a, b))
	require.True(t, EqualCauses(b, a))
}

func wrapErrorInXerrors(e error) error {
	return xerrors.Errorf("wrapped error: %w", e)
}

func TestEqualCausesGolangErrorAndXErrorUnequal(t *testing.T) {
	a := multipleErrorsGenerator(101)
	b := multipleErrorsGenerator(11)

	require.False(t, EqualCauses(a, b))
	require.False(t, EqualCauses(b, a))
}
