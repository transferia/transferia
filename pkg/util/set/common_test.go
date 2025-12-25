package set

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetContains(t *testing.T) {
	set := New[int]()
	testSetContains(t, set)
}

func TestSyncSetContains(t *testing.T) {
	set := NewSyncSet[int]()
	testSetContains(t, set)
}

func testSetContains[T AbstractSet[int]](t *testing.T, s T) {
	s.Add([]int{1, 3}...)
	require.True(t, s.Contains(1))
	require.False(t, s.Contains(2))
	require.True(t, s.Contains(3))

	s.Remove(1)
	require.False(t, s.Contains(1))

	s.Add(2)
	require.True(t, s.Contains(2))
}

//---

func TestSetString(t *testing.T) {
	set := New[int]()
	testSetString(t, set)
}

func TestSyncSetString(t *testing.T) {
	set := NewSyncSet[int]()
	testSetString(t, set)
}

func testSetString[T AbstractSet[int]](t *testing.T, s T) {
	var empty []int
	require.Equal(t, fmt.Sprint(empty), fmt.Sprint(New[int](empty...)))

	single := []int{1}
	require.Equal(t, fmt.Sprint(single), fmt.Sprint(New[int](single...)))

	var multiple []string
	for _, permutation := range permutations([]int{1, 2, 3}) {
		multiple = append(multiple, fmt.Sprint(permutation))
	}
	require.Contains(t, multiple, fmt.Sprint(New[int](1, 2, 3)))
}

func permutations(items []int) [][]int {
	if len(items) == 0 {
		return nil
	}
	if len(items) == 1 {
		return [][]int{items}
	}
	var result [][]int
	for _, basePermutation := range permutations(items[1:]) {
		for i := range basePermutation {
			var permutation []int
			permutation = append(permutation, basePermutation[:i]...)
			permutation = append(permutation, items[0])
			permutation = append(permutation, basePermutation[i:]...)
			result = append(result, permutation)
		}
		basePermutation = append(basePermutation, items[0])
		result = append(result, basePermutation)
	}
	return result
}
