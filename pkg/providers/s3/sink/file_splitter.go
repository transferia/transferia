package sink

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/filesplitter"
)

// FileSplitter manages file rotation for S3 snapshot uploads.
// It tracks row counts and byte sizes per logical file stream (S3ObjectRef)
// and determines when to rotate to a new file based on configured limits.
type wrappedFileSplitter struct {
	inner             *filesplitter.Splitter
	physicalKeyOwners map[string]string
	tablePathOwners   map[string]string
}

func (f *wrappedFileSplitter) reservePhysicalKeyNamespace(ref S3ObjectRef) error {
	if err := reservePhysicalTablePath(f.tablePathOwners, ref); err != nil {
		return xerrors.Errorf("reserve physical table path: %w", err)
	}
	if err := reservePhysicalKeyNamespace(f.physicalKeyOwners, ref); err != nil {
		return xerrors.Errorf("reserve physical object-key namespace: %w", err)
	}
	return nil
}

func reservePhysicalKeyNamespace(owners map[string]string, ref S3ObjectRef) error {
	if err := ref.Validate(); err != nil {
		return xerrors.Errorf("validate S3 object reference: %w", err)
	}
	physicalKey := ref.FullKey(0)
	logicalStream := ref.FileStreamKey()
	if owner, ok := owners[physicalKey]; ok && owner != logicalStream {
		return xerrors.Errorf("S3 object-key namespace collision for %q between distinct logical streams", physicalKey)
	}
	owners[physicalKey] = logicalStream
	return nil
}

func reservePhysicalTablePath(owners map[string]string, ref S3ObjectRef) error {
	if err := ref.Validate(); err != nil {
		return xerrors.Errorf("validate S3 object reference: %w", err)
	}
	physicalPath := ref.basePath()
	logicalTable := ref.namespace + "\x00" + ref.tableName
	if owner, ok := owners[physicalPath]; ok && owner != logicalTable {
		return xerrors.Errorf("S3 table-path collision for %q between distinct logical tables", physicalPath)
	}
	owners[physicalPath] = logicalTable
	return nil
}

func clonePhysicalKeyOwners(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, owner := range source {
		result[key] = owner
	}
	return result
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
		inner:             filesplitter.New(maxItemsPerFile, maxBytesPerFile),
		physicalKeyOwners: make(map[string]string),
		tablePathOwners:   make(map[string]string),
	}
}
