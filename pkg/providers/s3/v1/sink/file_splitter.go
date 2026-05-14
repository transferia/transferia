package sink

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/filesplitter"
)

// FileSplitter manages file rotation for S3 snapshot uploads.
// It tracks row counts and byte sizes per logical file stream (S3ObjectRef)
// and determines when to rotate to a new file based on configured limits.
type wrappedFileSplitter struct {
	inner *filesplitter.Splitter
}

// increaseKey initializes or rotates the file counter for the given ref.
// On first call for a ref, it initializes the counter to 0.
// On subsequent calls, it increments the counter and resets row/byte trackers.
// Returns the full S3 key for the new file.
func (f *wrappedFileSplitter) increaseKey(ref S3ObjectRef) string {
	idx := f.inner.StartNewFile(ref.FileStreamKey())
	return ref.FullKey(idx)
}

// key returns the current full S3 key for the given ref.
func (f *wrappedFileSplitter) key(ref S3ObjectRef) string {
	counter := f.inner.KeyNumber(ref.FileStreamKey())
	return ref.FullKey(counter)
}

// keyNumber returns the current counter value for the given ref.
func (f *wrappedFileSplitter) keyNumber(ref S3ObjectRef) int {
	return f.inner.KeyNumber(ref.FileStreamKey())
}

// addItems adds items to the current file for the given ref.
// It respects both row count and byte size limits.
// If both limits are configured, rotation happens when either limit is reached first.
// Returns the number of items that can be written before a limit is hit.
// At least one item is always accepted per file to prevent infinite loops.
func (f *wrappedFileSplitter) addItems(ref S3ObjectRef, items []*abstract.ChangeItem) int {
	return f.inner.AddItems(ref.FileStreamKey(), items)
}

func newFileSplitter(maxItemsPerFile int, maxBytesPerFile int) *wrappedFileSplitter {
	return &wrappedFileSplitter{
		inner: filesplitter.New(maxItemsPerFile, maxBytesPerFile),
	}
}
