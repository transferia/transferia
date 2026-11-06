package progress_tracker

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type failingPartsUpdater struct{}

func (*failingPartsUpdater) UpdateOperationTablesParts(string, []*abstract.OperationTablePart) error {
	return xerrors.New("UpdateOperationTablesParts")
}

func TestTableTrackerFlush(t *testing.T) {
	tracker := &TableTracker{
		cancel:    nil,
		wg:        sync.WaitGroup{},
		closeOnce: &sync.Once{},

		updater:             &failingPartsUpdater{},
		operationID:         "dtt",
		parts:               map[string]*abstract.OperationTablePart{"a": {}},
		progressUpdateMutex: &sync.Mutex{},
	}

	// check 'false'
	err := tracker.Flush(false)
	require.NoError(t, err)

	// check 'true'
	wg := sync.WaitGroup{}
	wg.Add(1)
	startTime := time.Now()
	go func() {
		defer wg.Done()

		defer func() {
			_ = recover()
		}()

		_ = tracker.Flush(true)
		endTime := time.Now()
		duration := endTime.Sub(startTime)
		require.True(t, duration > 4*time.Second)
	}()
	time.Sleep(5 * time.Second)
	tracker.updater = nil // trigger nil-pointer panic
	wg.Wait()
}

type recordingPartsUpdater struct {
	operationID string
	updates     [][]*abstract.OperationTablePart
}

func (s *recordingPartsUpdater) UpdateOperationTablesParts(operationID string, parts []*abstract.OperationTablePart) error {
	s.operationID = operationID
	s.updates = append(s.updates, parts)
	return nil
}

func TestTableTrackerPartLifecycle(t *testing.T) {
	updater := &recordingPartsUpdater{}
	tracker := &TableTracker{
		updater:             updater,
		operationID:         "operation",
		parts:               make(map[string]*abstract.OperationTablePart),
		progressUpdateMutex: &sync.Mutex{},
	}
	part := &abstract.OperationTablePart{Name: "table", Completed: true, CompletedRows: 42}
	tracker.Start(part)
	require.False(t, part.Completed)
	require.Zero(t, part.CompletedRows)
	tracker.IncCompletedRows(part, 7)
	tracker.Complete(part)
	require.Empty(t, updater.updates, "marking completion must not persist progress")
	require.NoError(t, tracker.Flush(true))
	require.Empty(t, tracker.parts)
	require.Equal(t, "operation", updater.operationID)
	require.Len(t, updater.updates, 1)
	require.Len(t, updater.updates[0], 1)
	require.NotSame(t, part, updater.updates[0][0])
	require.True(t, updater.updates[0][0].Completed)
	require.EqualValues(t, 7, updater.updates[0][0].CompletedRows)

	// A retry must register the part again and leave the persisted snapshot intact.
	tracker.Start(part)
	require.False(t, part.Completed)
	require.Zero(t, part.CompletedRows)
	require.NoError(t, tracker.Flush(false))
	require.Contains(t, tracker.parts, part.Key())
	require.Len(t, updater.updates, 2)
	require.False(t, updater.updates[1][0].Completed)
	require.Zero(t, updater.updates[1][0].CompletedRows)
	require.True(t, updater.updates[0][0].Completed)
	require.EqualValues(t, 7, updater.updates[0][0].CompletedRows)

	tracker.Complete(part)
	require.NoError(t, tracker.Flush(true))
	require.Empty(t, tracker.parts)
	require.Len(t, updater.updates, 3)
	require.True(t, updater.updates[2][0].Completed)
}

func TestTableTrackerIncCompletedRows(t *testing.T) {
	updater := &recordingPartsUpdater{}
	tracker := &TableTracker{
		updater:             updater,
		operationID:         "operation",
		parts:               make(map[string]*abstract.OperationTablePart),
		progressUpdateMutex: &sync.Mutex{},
	}
	part := &abstract.OperationTablePart{Name: "table", CompletedRows: 42}
	tracker.Add(part)
	tracker.IncCompletedRows(part, 7)
	tracker.IncCompletedRows(part, 3)

	require.NoError(t, tracker.Flush(false))
	require.Len(t, updater.updates, 1)
	require.Len(t, updater.updates[0], 1)
	require.EqualValues(t, 52, updater.updates[0][0].CompletedRows)
	require.False(t, updater.updates[0][0].Completed)
}
