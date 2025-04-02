package slicesx

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCornerCases(t *testing.T) {
	// nil slice
	require.Equal(t, [][]int(nil), SplitToChunks([]int(nil), -1))
	require.Equal(t, [][]int{}, SplitToChunks([]int(nil), 0))
	require.Equal(t, [][]int{nil}, SplitToChunks([]int(nil), 1))
	require.Equal(t, [][]int{nil, nil, nil}, SplitToChunks([]int(nil), 3))

	// empty slice
	require.Equal(t, [][]int(nil), SplitToChunks([]int{}, -1))
	require.Equal(t, [][]int{}, SplitToChunks([]int{}, 0))
	require.Equal(t, [][]int{nil}, SplitToChunks([]int{}, 1))
	require.Equal(t, [][]int{nil, nil, nil}, SplitToChunks([]int{}, 3))
}

func TestXxx(t *testing.T) {
	slice := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	cases := map[int][][]int{
		-1: nil,
		0:  {},
		1:  {{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		2:  {{0, 1, 2, 3, 4}, {5, 6, 7, 8, 9}},
		3:  {{0, 1, 2, 3}, {4, 5, 6, 7}, {8, 9}},
		4:  {{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9}},
		5:  {{0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9}},
		6:  {{0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9}, nil},
		7:  {{0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9}, nil, nil},
		8:  {{0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9}, nil, nil, nil},
		9:  {{0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9}, nil, nil, nil, nil},
		10: {[]int{0}, []int{1}, []int{2}, []int{3}, []int{4}, []int{5}, []int{6}, []int{7}, []int{8}, []int{9}},
		11: {[]int{0}, []int{1}, []int{2}, []int{3}, []int{4}, []int{5}, []int{6}, []int{7}, []int{8}, []int{9}, nil},
	}
	for n, expected := range cases {
		require.Equal(t, expected, SplitToChunks(slice, n), fmt.Sprintf("n = %d", n))
	}
}
