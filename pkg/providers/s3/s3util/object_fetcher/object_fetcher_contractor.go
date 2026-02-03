package object_fetcher

import (
	"fmt"
	"strings"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/logging/batching_logger"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

// ObjectFetcherContractor - check contracts:
//     * FetchObjects() should be called only when ObjectFetcher is empty
//     * Commit() should be called only for known filenames (which is previously extracted via 'FetchObjects()')

type ObjectFetcherContractor struct {
	impl ObjectFetcher

	logger log.Logger

	mu        sync.Mutex
	fileNames *set.Set[string]
}

func (w *ObjectFetcherContractor) RunBackgroundThreads(errCh chan error) {
	w.impl.RunBackgroundThreads(errCh)
}

func (w *ObjectFetcherContractor) FetchObjects(reader reader.Reader) ([]file.File, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.fileNames.Empty() {
		return nil, xerrors.Errorf("contract is broken - FetchObjects should be called only when all previous objects are committed - left %d files, files:%v", w.fileNames.Len(), w.fileNames.Slice())
	}
	result, err := w.impl.FetchObjects(reader)
	for _, el := range result {
		w.fileNames.Add(el.FileName)
	}
	return result, err
}

func (w *ObjectFetcherContractor) Commit(fileName string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.fileNames.Contains(fileName) {
		batching_logger.LogLine(batching_logger.NewAbsentThrottler(), func(in string) { w.logger.Info(in) }, fmt.Sprintf("%d files: %s", w.fileNames.Len(), strings.Join(w.fileNames.Slice(), ",")))
		return xerrors.Errorf("unknown file name: %s", fileName)
	}
	w.fileNames.Remove(fileName)

	return w.impl.Commit(fileName)
}

func (w *ObjectFetcherContractor) Close() error {
	return w.impl.Close()
}

func NewObjectFetcherContractor(logger log.Logger, in ObjectFetcher) *ObjectFetcherContractor {
	return &ObjectFetcherContractor{
		impl:      in,
		logger:    logger,
		mu:        sync.Mutex{},
		fileNames: set.New[string](),
	}
}
