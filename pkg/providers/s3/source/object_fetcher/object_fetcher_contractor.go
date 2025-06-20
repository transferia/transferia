//go:build !disable_s3_provider

package objectfetcher

import (
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/util/set"
)

// ObjectFetcherContractor - check contracts
// TODO - describe what it checks

type ObjectFetcherContractor struct {
	impl ObjectFetcher

	mu        sync.Mutex
	fileNames *set.Set[string]
}

func (w *ObjectFetcherContractor) RunBackgroundThreads(errCh chan error) {
	w.impl.RunBackgroundThreads(errCh)
}

func (w *ObjectFetcherContractor) FetchObjects(reader reader.Reader) ([]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.fileNames.Empty() {
		return nil, xerrors.Errorf("contract is broken - FetchObjects should be called only when all previous objects are committed - left %d files, files:%v", w.fileNames.Len(), w.fileNames.Slice())
	}
	result, err := w.impl.FetchObjects(reader)
	for _, el := range result {
		w.fileNames.Add(el)
	}
	return result, err
}

func (w *ObjectFetcherContractor) Commit(fileName string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.fileNames.Contains(fileName) {
		return xerrors.Errorf("unknown file name: %s", fileName)
	}
	w.fileNames.Remove(fileName)

	return w.impl.Commit(fileName)
}

func (w *ObjectFetcherContractor) FetchAndCommitAll(reader reader.Reader) error {
	return w.impl.FetchAndCommitAll(reader)
}

func NewObjectFetcherContractor(in ObjectFetcher) *ObjectFetcherContractor {
	return &ObjectFetcherContractor{
		impl:      in,
		mu:        sync.Mutex{},
		fileNames: set.New[string](),
	}
}
