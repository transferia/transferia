package sequencer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOffsetsToRanges(t *testing.T) {
	longOffsets := make([]int64, 100)
	for i := range longOffsets {
		longOffsets[i] = int64(i + 1)
	}

	tests := []struct {
		name     string
		offsets  []int64
		expected string
	}{
		{"nil slice", nil, ""},
		{"empty slice", []int64{}, ""},
		{"single", []int64{5}, "5"},
		{"two consecutive", []int64{10, 11}, "10-11"},
		{"two non-consecutive", []int64{10, 20}, "10,20"},
		{"mixed ranges and singles", []int64{1, 2, 3, 5, 6, 8}, "1-3,5-6,8"},
		{"long consecutive range", longOffsets, "1-100"},
		{"all separate", []int64{1, 3, 5, 7}, "1,3,5,7"},
		{"consecutive pairs", []int64{1, 2, 5, 6, 9, 10}, "1-2,5-6,9-10"},
		{"range at end", []int64{1, 3, 4, 5}, "1,3-5"},
		{"range at start", []int64{1, 2, 3, 4, 6}, "1-4,6"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, OffsetsToRanges(tt.offsets))
		})
	}
}
