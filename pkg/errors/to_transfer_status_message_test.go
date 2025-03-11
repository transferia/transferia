package errors

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/errors/categories"
)

const (
	testErrorText1 string = "error_1"
	testErrorText2 string = "error_2: "
	testErrorText3 string = "error_3: "
	testErrorText4 string = "error_4: "
)

func TestToAPIWarningEventOneLevelError(t *testing.T) {
	err := xerrors.New(testErrorText1)

	result := ToTransferStatusMessage(err)

	require.NotNil(t, result)
	require.Equal(t, testErrorText1, result.Message)
	require.Equal(t, testErrorText1, result.Heading)
	require.Equal(t, result.Categories, []string{categories.Internal.ID()})
}

func TestToAPIWarningEventTwoLevelError(t *testing.T) {
	err1 := xerrors.New(testErrorText1)
	err := xerrors.Errorf(testErrorText2+"%w", err1)

	result := ToTransferStatusMessage(err)

	require.NotNil(t, result)
	require.Equal(t, testErrorText2+testErrorText1, result.Message)
	require.Equal(t, testErrorText2+testErrorText1, result.Heading)
	require.Equal(t, result.Categories, []string{categories.Internal.ID()})
}

func TestToAPIWarningEventThreeLevelError(t *testing.T) {
	err1 := xerrors.New(testErrorText1)
	err2 := xerrors.Errorf(testErrorText2+"%w", err1)
	err := xerrors.Errorf(testErrorText3+"%w", err2)

	result := ToTransferStatusMessage(err)

	require.NotNil(t, result)
	require.Equal(t, testErrorText3+testErrorText2+testErrorText1, result.Message)
	require.Equal(t, testErrorText2+testErrorText1, result.Heading)
	require.Equal(t, result.Categories, []string{categories.Internal.ID()})
}

func TestToAPIWarningEventFourLevelError(t *testing.T) {
	err1 := xerrors.New(testErrorText1)
	err2 := xerrors.Errorf(testErrorText2+"%w", err1)
	err3 := xerrors.Errorf(testErrorText3+"%w", err2)
	err := xerrors.Errorf(testErrorText4+"%w", err3)

	result := ToTransferStatusMessage(err)

	require.NotNil(t, result)
	require.Equal(t, testErrorText4+testErrorText3+testErrorText2+testErrorText1, result.Message)
	require.Equal(t, testErrorText2+testErrorText1, result.Heading)
	require.Equal(t, result.Categories, []string{categories.Internal.ID()})
}

func TestToAPIWarningEventFourErrorsWithOneCategory(t *testing.T) {
	err1 := xerrors.New(testErrorText1)
	err2 := CategorizedErrorf(categories.Internal, testErrorText2+"%w", err1)
	err3 := xerrors.Errorf(testErrorText3+"%w", err2)
	err := xerrors.Errorf(testErrorText4+"%w", err3)

	result := ToTransferStatusMessage(err)

	require.NotNil(t, result)
	require.Equal(t, testErrorText4+testErrorText3+testErrorText2+testErrorText1, result.Message)
	require.Equal(t, testErrorText2+testErrorText1, result.Heading)
	require.Equal(t, result.Categories, []string{categories.Internal.ID()})
}

func TestToAPIWarningEventFourErrorsWithTwoCategories(t *testing.T) {
	err1 := xerrors.New(testErrorText1)
	err2 := CategorizedErrorf(categories.Internal, testErrorText2+"%w", err1)
	err3 := xerrors.Errorf(testErrorText3+"%w", err2)
	err := CategorizedErrorf(categories.Source, testErrorText4+"%w", err3)

	result := ToTransferStatusMessage(err)

	require.NotNil(t, result)
	require.Equal(t, testErrorText4+testErrorText3+testErrorText2+testErrorText1, result.Message)
	require.Equal(t, testErrorText2+testErrorText1, result.Heading)
	require.Equal(t, result.Categories, []string{categories.Internal.ID()})
}

func TestToAPIWarningNilError(t *testing.T) {
	var err error = nil

	result := ToTransferStatusMessage(err)

	require.NotNil(t, result)
	require.Equal(t, "", result.Message)
	require.Equal(t, "", result.Heading)
	require.Len(t, result.Categories, 0)
}
