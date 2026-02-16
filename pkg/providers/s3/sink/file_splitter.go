package sink

import (
	"github.com/transferia/transferia/pkg/abstract"
)

// FileSplitter manages file rotation for S3 snapshot uploads.
// It tracks row counts and byte sizes per logical file stream (s3ObjectRef)
// and determines when to rotate to a new file based on configured limits.
type FileSplitter struct {
	counterByRef map[s3ObjectRef]int
	rowsByRef    map[s3ObjectRef]int
	bytesByRef   map[s3ObjectRef]int

	maxItemsPerFile int
	maxBytesPerFile int
}

// increaseKey initializes or rotates the file counter for the given ref.
// On first call for a ref, it initializes the counter to 0.
// On subsequent calls, it increments the counter and resets row/byte trackers.
// Returns the full S3 key for the new file.
func (f *FileSplitter) increaseKey(ref s3ObjectRef) string {
	counter, ok := f.counterByRef[ref]
	if !ok {
		f.counterByRef[ref] = 0
		f.rowsByRef[ref] = 0
		f.bytesByRef[ref] = 0
		return ref.fullKey(0)
	}

	newCounter := counter + 1
	f.counterByRef[ref] = newCounter
	f.rowsByRef[ref] = 0
	f.bytesByRef[ref] = 0
	return ref.fullKey(newCounter)
}

// key returns the current full S3 key for the given ref.
func (f *FileSplitter) key(ref s3ObjectRef) string {
	counter, ok := f.counterByRef[ref]
	if !ok {
		return ref.fullKey(0)
	}
	return ref.fullKey(counter)
}

// keyNumber returns the current counter value for the given ref.
func (f *FileSplitter) keyNumber(ref s3ObjectRef) int {
	return f.counterByRef[ref]
}

// addItems adds items to the current file for the given ref.
// It respects both row count and byte size limits.
// If both limits are configured, rotation happens when either limit is reached first.
// Returns the number of items that can be written before a limit is hit.
// At least one item is always accepted per file to prevent infinite loops.
func (f *FileSplitter) addItems(ref s3ObjectRef, items []*abstract.ChangeItem) int {
	if len(items) == 0 {
		return 0
	}
	if _, ok := f.rowsByRef[ref]; !ok {
		return 0
	}

	hasRowLimit := f.maxItemsPerFile > 0
	hasByteLimit := f.maxBytesPerFile > 0

	if !hasRowLimit && !hasByteLimit {
		// No limits — accept all items
		return len(items)
	}

	count := 0
	for _, item := range items {
		// Check row limit
		if hasRowLimit && f.rowsByRef[ref] >= f.maxItemsPerFile {
			break
		}
		// Check byte limit; always allow at least one item per file
		itemBytes := int(item.Size.Read)
		if hasByteLimit && f.rowsByRef[ref] > 0 && f.bytesByRef[ref]+itemBytes > f.maxBytesPerFile {
			break
		}

		f.rowsByRef[ref]++
		f.bytesByRef[ref] += itemBytes
		count++
	}

	return count
}

func newFileSplitter(maxItemsPerFile int, maxBytesPerFile int) *FileSplitter {
	return &FileSplitter{
		counterByRef:    make(map[s3ObjectRef]int),
		rowsByRef:       make(map[s3ObjectRef]int),
		bytesByRef:      make(map[s3ObjectRef]int),
		maxItemsPerFile: maxItemsPerFile,
		maxBytesPerFile: maxBytesPerFile,
	}
}
