package filesplitter

import (
	"github.com/transferia/transferia/pkg/abstract"
)

// Splitter manages file rotation for batched change-item writes (S3 snapshots,
// Iceberg parquet, etc.). It tracks row counts and approximate byte sizes per
// logical stream (identified by streamKey) and determines how many items fit
// into the current file before limits are hit.
type Splitter struct {
	counterByRef map[string]int
	rowsByRef    map[string]int
	bytesByRef   map[string]int

	maxItemsPerFile int
	maxBytesPerFile int
}

// New creates a splitter with the given per-file limits. Zero means unlimited.
func New(maxItemsPerFile, maxBytesPerFile int) *Splitter {
	return &Splitter{
		counterByRef:    make(map[string]int),
		rowsByRef:       make(map[string]int),
		bytesByRef:      make(map[string]int),
		maxItemsPerFile: maxItemsPerFile,
		maxBytesPerFile: maxBytesPerFile,
	}
}

// StartNewFile initializes or rotates to the next file for streamKey.
// On the first call for streamKey it sets the file index to 0; on later calls it
// increments the index and resets row/byte trackers for the new file.
// Returns the 0-based file index (same as KeyNumber after the call).
func (f *Splitter) StartNewFile(streamKey string) int {
	counter, ok := f.counterByRef[streamKey]
	if !ok {
		f.counterByRef[streamKey] = 0
		f.rowsByRef[streamKey] = 0
		f.bytesByRef[streamKey] = 0
		return 0
	}
	newCounter := counter + 1
	f.counterByRef[streamKey] = newCounter
	f.rowsByRef[streamKey] = 0
	f.bytesByRef[streamKey] = 0
	return newCounter
}

// KeyNumber returns the current 0-based file index for streamKey.
func (f *Splitter) KeyNumber(streamKey string) int {
	return f.counterByRef[streamKey]
}

// AddItems returns how many leading items from items can be written to the
// current file. Requires StartNewFile(streamKey) to have been called first.
func (f *Splitter) AddItems(streamKey string, items []*abstract.ChangeItem) int {
	return f.addItemsBounded(streamKey, len(items), func(i int) int {
		if items[i] == nil {
			return 0
		}
		return int(items[i].Size.Read)
	})
}

// AddChangeItemsValues is like AddItems but for value slices (avoids allocating a
// pointer slice when the caller already has []abstract.ChangeItem).
func (f *Splitter) AddChangeItemsValues(streamKey string, items []abstract.ChangeItem) int {
	return f.addItemsBounded(streamKey, len(items), func(i int) int {
		return int(items[i].Size.Read)
	})
}

func (f *Splitter) addItemsBounded(streamKey string, n int, sizeAt func(i int) int) int {
	if n == 0 {
		return 0
	}
	if _, ok := f.rowsByRef[streamKey]; !ok {
		return 0
	}

	hasRowLimit := f.maxItemsPerFile > 0
	hasByteLimit := f.maxBytesPerFile > 0

	if !hasRowLimit && !hasByteLimit {
		return n
	}

	count := 0
	for i := 0; i < n; i++ {
		if hasRowLimit && f.rowsByRef[streamKey] >= f.maxItemsPerFile {
			break
		}
		itemBytes := sizeAt(i)
		if hasByteLimit && f.rowsByRef[streamKey] > 0 && f.bytesByRef[streamKey]+itemBytes > f.maxBytesPerFile {
			break
		}

		f.rowsByRef[streamKey]++
		f.bytesByRef[streamKey] += itemBytes
		count++
	}

	return count
}
