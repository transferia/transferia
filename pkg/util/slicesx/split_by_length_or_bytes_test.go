package slicesx

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func myStructSizeUint(s myStruct) uint64 { return uint64(s.Size) }

func TestSplitByBytesOrLength_Basic(t *testing.T) {
	in := []myStruct{{"a", 3}, {"b", 4}, {"c", 2}, {"d", 1}}
	actual, bytes, length := SplitByBytesOrLength(in, myStructSizeUint, 0, 0, 10, math.MaxUint64)
	expected := [][]myStruct{{{"a", 3}, {"b", 4}, {"c", 2}, {"d", 1}}}
	require.Equal(t, expected, actual)
	require.Equal(t, uint64(0), bytes)
	require.Equal(t, uint64(0), length)
}

func TestSplitByBytesOrLength_ExactByteLimitClosesChunk(t *testing.T) {
	in := []myStruct{{"a", 6}, {"b", 4}, {"c", 1}}
	actual, bytes, length := SplitByBytesOrLength(in, myStructSizeUint, 0, 0, 10, math.MaxUint64)
	expected := [][]myStruct{{{"a", 6}, {"b", 4}}, {{"c", 1}}}
	require.Equal(t, expected, actual)
	require.Equal(t, uint64(1), bytes)
	require.Equal(t, uint64(1), length)
}

func TestSplitByBytesOrLength_ExceedByteLimitKeepsOverflowInChunk(t *testing.T) {
	in := []myStruct{{"a", 6}, {"b", 5}, {"c", 1}}
	actual, _, _ := SplitByBytesOrLength(in, myStructSizeUint, 0, 0, 10, math.MaxUint64)
	expected := [][]myStruct{{{"a", 6}, {"b", 5}}, {{"c", 1}}}
	require.Equal(t, expected, actual)
}

func TestSplitByBytesOrLength_OversizeElementStaysInClosingChunk(t *testing.T) {
	in := []myStruct{{"a", 5}, {"big", 20}, {"b", 3}}
	actual, bytes, length := SplitByBytesOrLength(in, myStructSizeUint, 0, 0, 10, math.MaxUint64)
	expected := [][]myStruct{{{"a", 5}, {"big", 20}}, {{"b", 3}}}
	require.Equal(t, expected, actual)
	require.Equal(t, uint64(3), bytes)
	require.Equal(t, uint64(1), length)
}

func TestSplitByBytesOrLength_ZeroLimitsReturnsWholeInput(t *testing.T) {
	in := []myStruct{{"a", 5}, {"b", 10}}
	actual, bytes, length := SplitByBytesOrLength(in, myStructSizeUint, 3, 2, 0, 0)
	expected := [][]myStruct{in}
	require.Equal(t, expected, actual)
	require.Equal(t, uint64(3), bytes)
	require.Equal(t, uint64(2), length)
}

func TestSplitByBytesOrLength_LengthLimit(t *testing.T) {
	in := []myStruct{{"a", 3}, {"b", 4}, {"c", 2}, {"d", 1}}
	actual, bytes, length := SplitByBytesOrLength(in, myStructSizeUint, 0, 0, math.MaxUint64, 2)
	expected := [][]myStruct{{{"a", 3}, {"b", 4}}, {{"c", 2}, {"d", 1}}}
	require.Equal(t, expected, actual)
	require.Equal(t, uint64(0), bytes)
	require.Equal(t, uint64(0), length)
}

func TestSplitByBytesOrLength_NilInput(t *testing.T) {
	actual, bytes, length := SplitByBytesOrLength[myStruct](nil, myStructSizeUint, 5, 2, 10, 100)
	require.Nil(t, actual)
	require.Equal(t, uint64(5), bytes)
	require.Equal(t, uint64(2), length)
}

func TestSplitByBytesOrLength_EmptyInput(t *testing.T) {
	actual, bytes, length := SplitByBytesOrLength([]myStruct{}, myStructSizeUint, 7, 3, 10, 100)
	require.Nil(t, actual)
	require.Equal(t, uint64(7), bytes)
	require.Equal(t, uint64(3), length)
}

func TestSplitByBytesOrLength_WithInitialState(t *testing.T) {
	in := []myStruct{{"b", 5}, {"c", 1}}
	actual, bytes, length := SplitByBytesOrLength(in, myStructSizeUint, 6, 1, 10, math.MaxUint64)
	expected := [][]myStruct{{{"b", 5}}, {{"c", 1}}}
	require.Equal(t, expected, actual)
	require.Equal(t, uint64(1), bytes)
	require.Equal(t, uint64(1), length)
}

func TestSplitByBytesOrLength_ZeroSizeElements(t *testing.T) {
	in := []myStruct{{"a", 0}, {"b", 0}, {"c", 0}}
	actual, bytes, length := SplitByBytesOrLength(in, myStructSizeUint, 0, 0, 10, math.MaxUint64)
	expected := [][]myStruct{{{"a", 0}, {"b", 0}, {"c", 0}}}
	require.Equal(t, expected, actual)
	require.Equal(t, uint64(0), bytes)
	require.Equal(t, uint64(3), length)
}

func TestSplitByBytesOrLength_GenericInts(t *testing.T) {
	in := []int{3, 4, 2, 1, 5}
	actual, bytes, length := SplitByBytesOrLength(in, func(n int) uint64 { return uint64(n) }, 0, 0, 10, math.MaxUint64)
	expected := [][]int{{3, 4, 2, 1}, {5}}
	require.Equal(t, expected, actual)
	require.Equal(t, uint64(5), bytes)
	require.Equal(t, uint64(1), length)
}

func TestSplitByBytesOrLength_GenericStrings(t *testing.T) {
	in := []string{"hello", "world", "go", "is", "great"}
	actual, _, _ := SplitByBytesOrLength(in, func(s string) uint64 { return uint64(len(s)) }, 0, 0, 8, math.MaxUint64)
	expected := [][]string{{"hello", "world"}, {"go", "is", "great"}}
	require.Equal(t, expected, actual)
}

func TestSplitByBytesOrLength_LengthLimitOne(t *testing.T) {
	in := []myStruct{{"a", 1}, {"b", 1}}
	actual, _, _ := SplitByBytesOrLength(in, myStructSizeUint, 0, 0, math.MaxUint64, 1)
	expected := [][]myStruct{{{"a", 1}}, {{"b", 1}}}
	require.Equal(t, expected, actual)
}

func TestSplitByBytesOrLength_ByteLimitOne(t *testing.T) {
	in := []myStruct{{"a", 1}, {"b", 1}}
	actual, _, _ := SplitByBytesOrLength(in, myStructSizeUint, 0, 0, 1, math.MaxUint64)
	expected := [][]myStruct{{{"a", 1}}, {{"b", 1}}}
	require.Equal(t, expected, actual)
}

func TestSplitByBytesOrLength_LengthLimitTriggersBeforeBytes(t *testing.T) {
	in := []myStruct{{"a", 8}, {"b", 8}, {"c", 1}}
	actual, _, _ := SplitByBytesOrLength(in, myStructSizeUint, 0, 0, 20, 2)
	expected := [][]myStruct{{{"a", 8}, {"b", 8}}, {{"c", 1}}}
	require.Equal(t, expected, actual)
}
