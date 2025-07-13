package batcher

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

// uintPusher collects all numbers and returns error if any of values is negative.
func uintPusher(collector *[]int) func([]int) error {
	return func(values []int) error {
		*collector = append(*collector, values...)
		if slices.ContainsFunc(values, func(x int) bool { return x < 0 }) {
			return xerrors.New("negative value found")
		}
		return nil
	}
}

func TestAppendAndFlush(t *testing.T) {
	var collector []int
	batcher := NewBatcher(3, uintPusher(&collector))
	defer batcher.Close()

	// One batch in many Appends.
	require.NoError(t, batcher.Append([]int{1, 2}))
	require.NoError(t, batcher.Append([]int{3}))
	require.Equal(t, []int{1, 2, 3}, collector)

	// Many batches in one Append.
	require.NoError(t, batcher.Append([]int{4, 5, 6, 7, 8, 9, 10}))
	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, collector)

	// Flush of items smaller than batchSize.
	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, collector) // 10 is not pushed, bcs batch size not reached.
	require.NoError(t, batcher.Flush())
	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, collector) // 10 is pushed after force Flush call.
	require.NoError(t, batcher.Flush())
	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, collector) // 10 is pushed after force Flush call.

	// Test error.
	require.Error(t, batcher.Append([]int{11, -1, 12}))
	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, -1, 12}, collector)
}

func TestClose(t *testing.T) {
	var collector []int
	batcher := NewBatcher(3, uintPusher(&collector))

	require.NoError(t, batcher.Append([]int{1, 2, 3, 4, 5}))
	batcher.Close()

	require.Error(t, batcher.Append([]int{6, 7, 8}))
	require.Equal(t, []int{1, 2, 3}, collector)

	require.Error(t, batcher.Flush())
	require.Equal(t, []int{1, 2, 3}, collector)
}

func TestFlushAndClose(t *testing.T) {
	var collector []int
	batcher := NewBatcher(3, uintPusher(&collector))

	require.NoError(t, batcher.Append([]int{1, 2}))
	require.NoError(t, batcher.FlushAndClose())
	require.Equal(t, []int{1, 2}, collector)

	require.Error(t, batcher.Append([]int{3}))
}

func TestEmptyAppend(t *testing.T) {
	var collector []int
	batcher := NewBatcher(3, uintPusher(&collector))

	require.NoError(t, batcher.Append([]int{}))
	require.NoError(t, batcher.Append([]int{1, 2, 3}))
	require.Equal(t, []int{1, 2, 3}, collector)
}

func TestZeroBatchSize(t *testing.T) {
	var collector []int
	batcher := NewBatcher(0, uintPusher(&collector))

	require.NoError(t, batcher.Append([]int{1}))
	require.Equal(t, []int{1}, collector)

	require.NoError(t, batcher.Append([]int{2, 3}))
	require.Equal(t, []int{1, 2, 3}, collector)

	require.Error(t, batcher.Append([]int{4, -5, 6}))
	require.Equal(t, []int{1, 2, 3, 4, -5, 6}, collector)
}

func TestLargeAppend(t *testing.T) {
	var collector []int
	batcher := NewBatcher(3, uintPusher(&collector))

	largeData := make([]int, 1000)
	for i := range 1000 {
		largeData[i] = i + 1
	}

	require.NoError(t, batcher.Append(largeData))
	require.Equal(t, 999, len(collector)) // 333 batches processed.
	require.NoError(t, batcher.Flush())
	require.Equal(t, 1000, len(collector)) // Remaining single batch processed.
}
