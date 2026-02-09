package slicesx

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type myStruct struct {
	Name string
	Size int
}

func myStructSize(s myStruct) int { return s.Size }

func TestSplitBySize_Basic(t *testing.T) {
	in := []myStruct{{"a", 3}, {"b", 4}, {"c", 2}, {"d", 1}}
	actual, err := SplitBySize(in, 10, myStructSize, OversizePolicyAllow)
	require.NoError(t, err)
	expected := [][]myStruct{{{"a", 3}, {"b", 4}, {"c", 2}, {"d", 1}}}
	require.Equal(t, expected, actual)
}

func TestSplitBySize_ExactLimitStaysInChunk(t *testing.T) {
	in := []myStruct{{"a", 6}, {"b", 4}, {"c", 1}}
	actual, err := SplitBySize(in, 10, myStructSize, OversizePolicyAllow)
	require.NoError(t, err)
	expected := [][]myStruct{{{"a", 6}, {"b", 4}}, {{"c", 1}}}
	require.Equal(t, expected, actual)
}

func TestSplitBySize_ExceedLimitStartsNewChunk(t *testing.T) {
	in := []myStruct{{"a", 6}, {"b", 5}, {"c", 1}}
	actual, err := SplitBySize(in, 10, myStructSize, OversizePolicyAllow)
	require.NoError(t, err)
	expected := [][]myStruct{{{"a", 6}}, {{"b", 5}, {"c", 1}}}
	require.Equal(t, expected, actual)
}

func TestSplitBySize_OversizeAllow(t *testing.T) {
	in := []myStruct{{"a", 5}, {"big", 20}, {"b", 3}}
	actual, err := SplitBySize(in, 10, myStructSize, OversizePolicyAllow)
	require.NoError(t, err)
	expected := [][]myStruct{{{"a", 5}}, {{"big", 20}}, {{"b", 3}}}
	require.Equal(t, expected, actual)
}

func TestSplitBySize_OversizeError(t *testing.T) {
	in := []myStruct{{"a", 5}, {"big", 20}, {"b", 3}}
	actual, err := SplitBySize(in, 10, myStructSize, OversizePolicyError)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOversizedElement)
	require.Nil(t, actual)
}

func TestSplitBySize_EqualToLimitAllowedWithErrorPolicy(t *testing.T) {
	in := []myStruct{{"exact", 10}, {"a", 1}}
	actual, err := SplitBySize(in, 10, myStructSize, OversizePolicyError)
	require.NoError(t, err)
	expected := [][]myStruct{{{"exact", 10}}, {{"a", 1}}}
	require.Equal(t, expected, actual)
}

func TestSplitBySize_NilInput(t *testing.T) {
	actual, err := SplitBySize[myStruct](nil, 10, myStructSize, OversizePolicyAllow)
	require.NoError(t, err)
	require.Nil(t, actual)
}

func TestSplitBySize_EmptyInput(t *testing.T) {
	actual, err := SplitBySize([]myStruct{}, 10, myStructSize, OversizePolicyAllow)
	require.NoError(t, err)
	require.Nil(t, actual)
}

func TestSplitBySize_ZeroLimit(t *testing.T) {
	_, err := SplitBySize([]myStruct{{"a", 1}}, 0, myStructSize, OversizePolicyAllow)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNonPositiveLimit)
}

func TestSplitBySize_NegativeLimit(t *testing.T) {
	_, err := SplitBySize([]myStruct{{"a", 1}}, -5, myStructSize, OversizePolicyAllow)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNonPositiveLimit)
}

func TestSplitBySize_NilSizeFunc(t *testing.T) {
	_, err := SplitBySize([]myStruct{{"a", 1}}, 10, nil, OversizePolicyAllow)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilSizeFunc)
}

func TestSplitBySize_InvalidPolicy(t *testing.T) {
	_, err := SplitBySize([]myStruct{{"a", 11}}, 10, myStructSize, OversizePolicy(99))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidPolicy)
}

func TestSplitBySize_NegativeSizeFuncResult(t *testing.T) {
	_, err := SplitBySize([]myStruct{{"a", -1}}, 10, myStructSize, OversizePolicyAllow)
	require.Error(t, err)
}

func TestSplitBySize_ZeroSizeElements(t *testing.T) {
	in := []myStruct{{"a", 0}, {"b", 0}, {"c", 0}}
	actual, err := SplitBySize(in, 10, myStructSize, OversizePolicyAllow)
	require.NoError(t, err)
	expected := [][]myStruct{{{"a", 0}, {"b", 0}, {"c", 0}}}
	require.Equal(t, expected, actual)
}

func TestSplitBySize_GenericInts(t *testing.T) {
	in := []int{3, 4, 2, 1, 5}
	actual, err := SplitBySize(in, 10, func(n int) int { return n }, OversizePolicyAllow)
	require.NoError(t, err)
	expected := [][]int{{3, 4, 2, 1}, {5}}
	require.Equal(t, expected, actual)
}

func TestSplitBySize_GenericStrings(t *testing.T) {
	in := []string{"hello", "world", "go", "is", "great"}
	actual, err := SplitBySize(in, 8, func(s string) int { return len(s) }, OversizePolicyAllow)
	require.NoError(t, err)
	expected := [][]string{{"hello"}, {"world", "go"}, {"is", "great"}}
	require.Equal(t, expected, actual)
}

func TestSplitBySize_LimitOneAllOnes(t *testing.T) {
	in := []myStruct{{"a", 1}, {"b", 1}}
	actual, err := SplitBySize(in, 1, myStructSize, OversizePolicyAllow)
	require.NoError(t, err)
	expected := [][]myStruct{{{"a", 1}}, {{"b", 1}}}
	require.Equal(t, expected, actual)
}
