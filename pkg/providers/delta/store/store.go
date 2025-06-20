//go:build !disable_delta_provider

package store

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/iter"
)

var ErrFileNotFound = xerrors.New("file not found")

type StoreConfig interface {
	isStoreConfig()
}

// Store is general interface for all critical file system operations required to read and write
// the Delta logs. The correctness is predicated on the atomicity and durability guarantees
// of the implementation of this interface. Specifically,
// Consistent listing: Once a file has been written in a directory, all future listings for
// that directory must return that file.
// All subclasses of this interface is required to have a constructor that takes StoreConfig
// as a single parameter. This constructor is used to dynamically create the Store.
// Store and its implementations are not meant for direct access but for configuration based
// on storage system. See [[https://docs.delta.io/latest/delta-storage.html]] for details.
type Store interface {
	// Root return root path for delta-table store
	Root() string

	// Read the given file and return an `Iterator` of lines, with line breaks removed from each line.
	// Callers of this function are responsible to close the iterator if they are done with it.
	Read(path string) (iter.Iter[string], error)

	// ListFrom resolve the paths in the same directory that are lexicographically greater or equal to (UTF-8 sorting) the given `path`.
	// The result should also be sorted by the file name.
	ListFrom(path string) (iter.Iter[*FileMeta], error)
}

func New(config StoreConfig) (Store, error) {
	switch c := config.(type) {
	case *S3Config:
		return NewStoreS3(c)
	case *LocalConfig:
		return NewStoreLocal(c), nil
	default:
		return nil, xerrors.Errorf("unknown store config type: %T", config)
	}
}
