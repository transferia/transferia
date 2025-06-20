//go:build !disable_s3_provider

package sink

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
)

func fileCacheFromItems(items []abstract.ChangeItem) *FileCache {
	fc := newFileCache(abstract.TableID{Namespace: "", Name: "table"})
	for i := 0; i < len(items); i++ {
		_ = fc.Add(&items[i])
	}
	return fc
}

func createItemsRange(from, to uint64) []abstract.ChangeItem { // items are approximatelly 20 bytes
	items := make([]abstract.ChangeItem, 0)
	for i := from; i <= to; i++ {
		item := abstract.MakeRawMessage(
			"table",
			time.Time{},
			"test-topic",
			0,
			int64(i),
			[]byte(fmt.Sprintf("test_part_0_value_%v", i)),
		)
		items = append(items, item)
	}

	return items
}

func createItem(lsn uint64) []abstract.ChangeItem {
	return createItemsRange(lsn, lsn)
}

func checkCache(t *testing.T, cache *FileCache, expected []abstract.ChangeItem) {
	require.Equal(t, len(expected), len(cache.items))
	for i, item := range cache.items {
		require.Equal(t, expected[i], *item)
	}
}

func TestSplit(t *testing.T) {
	tests := []struct {
		name        string
		items       []abstract.ChangeItem
		intervals   []ObjectRange
		expected    [][]abstract.ChangeItem
		maxPartSize uint64
	}{
		// basics
		{
			name:        "cache_with_no_intervals",
			items:       createItemsRange(1, 10),
			intervals:   []ObjectRange{},
			expected:    [][]abstract.ChangeItem{},
			maxPartSize: 1000,
		},
		{
			name:        "cache_with_no_intervals_intersection_with_intervals",
			items:       createItemsRange(10, 20),
			intervals:   []ObjectRange{{From: 1, To: 5}, {From: 25, To: 30}},
			expected:    [][]abstract.ChangeItem{},
			maxPartSize: 1000,
		},
		{
			name:        "cache_with_full_range",
			items:       createItemsRange(1, 10),
			intervals:   []ObjectRange{{From: 1, To: 10}},
			expected:    [][]abstract.ChangeItem{createItemsRange(1, 10)},
			maxPartSize: 1000,
		},
		{
			name:        "cache_full_range_inside_interval",
			items:       createItemsRange(5, 10),
			intervals:   []ObjectRange{{From: 1, To: 15}},
			expected:    [][]abstract.ChangeItem{createItemsRange(5, 10)},
			maxPartSize: 1000,
		},
		// intervals
		{
			name:        "cache_with_full_range_by_2_intervals",
			items:       createItemsRange(1, 10),
			intervals:   []ObjectRange{{From: 1, To: 5}, {From: 6, To: 10}},
			expected:    [][]abstract.ChangeItem{createItemsRange(1, 10)},
			maxPartSize: 1000,
		},
		{
			name:        "cache_with_subranges_by_2_discrete_intervals",
			items:       createItemsRange(1, 10),
			intervals:   []ObjectRange{{From: 2, To: 3}, {From: 7, To: 8}},
			expected:    [][]abstract.ChangeItem{createItemsRange(2, 3), createItemsRange(7, 8)},
			maxPartSize: 1000,
		},
		{
			name:        "cache_with_subranges_by_2_discrete_intervals_on_borders",
			items:       createItemsRange(10, 20),
			intervals:   []ObjectRange{{From: 7, To: 12}, {From: 18, To: 23}},
			expected:    [][]abstract.ChangeItem{createItemsRange(10, 12), createItemsRange(18, 20)},
			maxPartSize: 1000,
		},
		{
			name:        "cache_full_range_split_by_size",
			items:       createItemsRange(1, 5),
			intervals:   []ObjectRange{{From: 1, To: 5}},
			expected:    [][]abstract.ChangeItem{createItem(1), createItem(2), createItem(3), createItem(4), createItem(5)},
			maxPartSize: 10,
		},
		{
			name:        "cache_with_subranges_by_all_kings_of_intervals",
			items:       createItemsRange(10, 20),
			intervals:   []ObjectRange{{From: 7, To: 11}, {From: 15, To: 16}, {From: 19, To: 23}},
			expected:    [][]abstract.ChangeItem{createItemsRange(10, 11), createItemsRange(15, 16), createItemsRange(19, 20)},
			maxPartSize: 1000,
		},
		{
			name:        "cache_with_subranges_all_scenarious",
			items:       createItemsRange(10, 20),
			intervals:   []ObjectRange{{From: 7, To: 11}, {From: 14, To: 16}, {From: 19, To: 23}},
			expected:    [][]abstract.ChangeItem{createItemsRange(10, 11), createItemsRange(14, 15), createItem(16), createItemsRange(19, 20)},
			maxPartSize: 45,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cache := fileCacheFromItems(tc.items)
			parts := cache.Split(tc.intervals, tc.maxPartSize)
			require.Equal(t, len(tc.expected), len(parts))
			for i, part := range parts {
				checkCache(t, part, tc.expected[i])
			}
		})
	}
}
