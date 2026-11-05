package s3_shared_memory

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/object_fetcher"
	"github.com/transferia/transferia/pkg/util/set"
)

type recordingObjectFetcher struct {
	object_fetcher.ObjectFetcher
	files       []file.File
	committed   []string
	commitError error
}

func (s *recordingObjectFetcher) FetchObjects(s3_reader.Reader) ([]file.File, error) {
	return s.files, nil
}

func (s *recordingObjectFetcher) Commit(fileName string) error {
	if s.commitError != nil {
		return s.commitError
	}
	for _, committedFile := range s.committed {
		if committedFile == fileName {
			return xerrors.Errorf("file already committed: %s", fileName)
		}
	}
	s.committed = append(s.committed, fileName)
	return nil
}

type failingWorkerDoneCoordinator struct {
	coordinator.Coordinator
	firstError    error
	setStateCalls int
}

func (c *failingWorkerDoneCoordinator) SetTransferState(transferID string, state map[string]*coordinator.TransferStateData) error {
	c.setStateCalls++
	if c.setStateCalls == 1 {
		return c.firstError
	}
	return c.Coordinator.SetTransferState(transferID, state)
}

func TestUpdateOperationTablesPartsCommitsOnePart(test *testing.T) {
	fetcher := &recordingObjectFetcher{}
	worker := new(S3SharedMemorySecondaryWorker)
	worker.poller = fetcher
	worker.completedFiles = set.New[string]()
	require.NoError(test, worker.UpdateOperationTablesParts("operation", []*abstract.OperationTablePart{
		{Completed: false, Filter: `not parsed until completed`},
	}))
	require.Empty(test, fetcher.committed)
	firstPart := new(abstract.OperationTablePart)
	firstPart.Completed = true
	firstPart.Filter = `["first"]`
	require.NoError(test, worker.UpdateOperationTablesParts("operation", []*abstract.OperationTablePart{firstPart}))
	require.NoError(test, worker.UpdateOperationTablesParts("operation", []*abstract.OperationTablePart{firstPart}))
	require.Equal(test, []string{"first"}, fetcher.committed)
	require.NoError(test, worker.UpdateOperationTablesParts("operation", []*abstract.OperationTablePart{
		{Completed: true, Filter: `["second"]`},
	}))
	require.Equal(test, []string{"first", "second"}, fetcher.committed)
}

func TestUpdateOperationTablesPartsRejectsInvalidPartCount(test *testing.T) {
	for _, testCase := range []struct {
		name  string
		parts []*abstract.OperationTablePart
	}{
		{name: "nil", parts: nil},
		{name: "empty", parts: []*abstract.OperationTablePart{}},
		{name: "multiple", parts: []*abstract.OperationTablePart{new(abstract.OperationTablePart), new(abstract.OperationTablePart)}},
	} {
		test.Run(testCase.name, func(test *testing.T) {
			fetcher := new(recordingObjectFetcher)
			worker := new(S3SharedMemorySecondaryWorker)
			worker.poller = fetcher
			require.ErrorContains(test, worker.UpdateOperationTablesParts("operation", testCase.parts), "len(tables)")
			require.Empty(test, fetcher.committed)
		})
	}
}

func TestUpdateOperationTablesPartsRejectsInvalidFilter(test *testing.T) {
	for _, filter := range []string{`invalid`, `[]`, `["first","second"]`} {
		test.Run(filter, func(test *testing.T) {
			fetcher := &recordingObjectFetcher{}
			worker := &S3SharedMemorySecondaryWorker{poller: fetcher}
			require.Error(test, worker.UpdateOperationTablesParts("operation", []*abstract.OperationTablePart{{Completed: true, Filter: filter}}))
			require.Empty(test, fetcher.committed)
		})
	}
}

func TestUpdateOperationTablesPartsPropagatesCommitError(test *testing.T) {
	fetcher := &recordingObjectFetcher{commitError: xerrors.New("commit failed")}
	worker := new(S3SharedMemorySecondaryWorker)
	worker.poller = fetcher
	worker.completedFiles = set.New[string]()
	part := new(abstract.OperationTablePart)
	part.Completed = true
	part.Filter = `["first"]`
	require.ErrorContains(test, worker.UpdateOperationTablesParts("operation", []*abstract.OperationTablePart{part}), "commit failed")
	require.Empty(test, fetcher.committed)
	fetcher.commitError = nil
	require.NoError(test, worker.UpdateOperationTablesParts("operation", []*abstract.OperationTablePart{part}))
	require.Equal(test, []string{"first"}, fetcher.committed)
}

func TestWorkerDoneAfterLoadingAllFiles(test *testing.T) {
	controlPlane := coordinator.NewStatefulFakeClient()
	fetcher := &recordingObjectFetcher{}
	worker := new(S3SharedMemorySecondaryWorker)
	worker.cfg = new(s3_model.S3Source)
	worker.cfg.TableName = "rows"
	worker.transferID = "transfer"
	worker.effectiveWorkerNum = effective_worker_num.NewEffectiveWorkerNumSingleWorker()
	worker.cp = controlPlane
	worker.lgr = logger.Log
	worker.poller = fetcher
	worker.files = []file.File{
		{FileName: "first", FileSize: 0, LastModifiedNS: 0},
		{FileName: `folder/"quoted".json`, FileSize: 0, LastModifiedNS: 0},
	}
	worker.completedFiles = set.New[string]()
	for _, fileName := range []string{"first", `folder/"quoted".json`} {
		part, err := worker.NextOperationTablePart(context.Background())
		require.NoError(test, err)
		require.NotNil(test, part)
		part.Completed = true
		require.NoError(test, worker.UpdateOperationTablesParts("operation", []*abstract.OperationTablePart{part}))
		require.Equal(test, fileName, fetcher.committed[len(fetcher.committed)-1])
		stateKeys, err := controlPlane.GetTransferStateKeys("transfer")
		require.NoError(test, err)
		require.Empty(test, stateKeys)
	}
	part, err := worker.NextOperationTablePart(context.Background())
	require.NoError(test, err)
	require.Nil(test, part)
	values, err := controlPlane.GetTransferStateByKeys("transfer", []string{"worker_done_0"})
	require.NoError(test, err)
	require.Equal(test, "true", values["worker_done_0"].Generic)
}

func TestNextOperationTablePartPropagatesWorkerDoneError(test *testing.T) {
	stateError := xerrors.New("worker done state unavailable")
	failingCoordinator := &failingWorkerDoneCoordinator{
		Coordinator:   coordinator.NewStatefulFakeClient(),
		firstError:    stateError,
		setStateCalls: 0,
	}
	worker := new(S3SharedMemorySecondaryWorker)
	worker.transferID = "transfer"
	worker.effectiveWorkerNum = effective_worker_num.NewEffectiveWorkerNumSingleWorker()
	worker.cp = failingCoordinator
	worker.lgr = logger.Log
	part, err := worker.NextOperationTablePart(context.Background())
	require.ErrorIs(test, err, stateError)
	require.Nil(test, part)
	require.Equal(test, 1, failingCoordinator.setStateCalls)
}

func TestConcurrentUpdatesDoNotCommitFileTwice(test *testing.T) {
	fileName := `folder/"quoted".json`
	fetcher := &recordingObjectFetcher{
		ObjectFetcher: nil,
		files:         []file.File{{FileName: fileName, FileSize: 0, LastModifiedNS: 0}},
		committed:     nil,
		commitError:   nil,
	}
	contractor := object_fetcher.NewObjectFetcherContractor(logger.Log, fetcher)
	files, err := contractor.FetchObjects(nil)
	require.NoError(test, err)
	source := new(s3_model.S3Source)
	source.TableName = "rows"
	worker := new(S3SharedMemorySecondaryWorker)
	worker.cfg = source
	worker.poller = contractor
	worker.files = files
	worker.completedFiles = set.New[string]()
	part, err := worker.NextOperationTablePart(context.Background())
	require.NoError(test, err)
	require.NotNil(test, part)
	part.Completed = true
	parts := []*abstract.OperationTablePart{part}

	const updateCount = 8
	start := make(chan struct{})
	errors := make(chan error, updateCount)
	var updates sync.WaitGroup
	for i := 0; i < updateCount; i++ {
		updates.Add(1)
		go func() {
			defer updates.Done()
			<-start
			errors <- worker.UpdateOperationTablesParts("operation", parts)
		}()
	}
	close(start)
	updates.Wait()
	close(errors)
	for err := range errors {
		require.NoError(test, err)
	}
	require.Equal(test, []string{fileName}, fetcher.committed)
}
