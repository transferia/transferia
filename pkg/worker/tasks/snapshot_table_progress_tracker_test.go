package tasks

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/worker/tasks/table_part_provider/shared_memory"
)

type sharedMemoryErrorable struct{}

func (s *sharedMemoryErrorable) Store(_ []*abstract.OperationTablePart) error {
	return xerrors.New("Store")
}
func (s *sharedMemoryErrorable) NextOperationTablePart(_ context.Context) (*abstract.OperationTablePart, error) {
	return nil, xerrors.New("NextOperationTablePart")
}
func (s *sharedMemoryErrorable) UpdateOperationTablesParts(_ string, _ []*abstract.OperationTablePart) error {
	return xerrors.New("UpdateOperationTablesParts")
}
func (s *sharedMemoryErrorable) Close() error {
	return xerrors.New("Close")
}
func (s *sharedMemoryErrorable) GetShardStateNoWait(ctx context.Context, operationID string) (string, error) {
	return "", xerrors.New("GetShardStateNoWait")
}
func (s *sharedMemoryErrorable) SetOperationState(operationID string, newState string) error {
	return xerrors.New("SetOperationState")
}

func TestSnapshotTableProgressTrackerFlush(t *testing.T) {
	tracker := &SnapshotTableProgressTracker{
		cancel:    nil,
		wg:        sync.WaitGroup{},
		closeOnce: &sync.Once{},

		sharedMemory:        &sharedMemoryErrorable{},
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
	tracker.sharedMemory = nil // trigger nil-pointer panic
	wg.Wait()
}

type recordingProgressSharedMemory struct {
	abstract.SharedMemory
	operationID string
	updates     [][]*abstract.OperationTablePart
}

var _ abstract.SharedMemory = (*recordingProgressSharedMemory)(nil)

func (s *recordingProgressSharedMemory) UpdateOperationTablesParts(operationID string, parts []*abstract.OperationTablePart) error {
	s.operationID = operationID
	s.updates = append(s.updates, parts)
	return nil
}

func TestSnapshotTableProgressTrackerPartLifecycle(t *testing.T) {
	memory := &recordingProgressSharedMemory{SharedMemory: shared_memory.NewLocal("operation")}
	tracker := &SnapshotTableProgressTracker{
		sharedMemory:        memory,
		operationID:         "operation",
		parts:               make(map[string]*abstract.OperationTablePart),
		progressUpdateMutex: &sync.Mutex{},
	}
	part := &abstract.OperationTablePart{Name: "table", Completed: true, CompletedRows: 42}
	tracker.Start(part)
	require.False(t, part.Completed)
	require.Zero(t, part.CompletedRows)
	part.CompletedRows = 7
	tracker.Complete(part)
	require.Empty(t, memory.updates, "marking completion must not persist progress")
	require.NoError(t, tracker.Flush(true))
	require.Empty(t, tracker.parts)
	require.Equal(t, "operation", memory.operationID)
	require.Len(t, memory.updates, 1)
	require.Len(t, memory.updates[0], 1)
	require.NotSame(t, part, memory.updates[0][0])
	require.True(t, memory.updates[0][0].Completed)
	require.EqualValues(t, 7, memory.updates[0][0].CompletedRows)

	// A retry must register the part again and leave the persisted snapshot intact.
	tracker.Start(part)
	require.False(t, part.Completed)
	require.Zero(t, part.CompletedRows)
	require.NoError(t, tracker.Flush(false))
	require.Contains(t, tracker.parts, part.Key())
	require.Len(t, memory.updates, 2)
	require.False(t, memory.updates[1][0].Completed)
	require.Zero(t, memory.updates[1][0].CompletedRows)
	require.True(t, memory.updates[0][0].Completed)
	require.EqualValues(t, 7, memory.updates[0][0].CompletedRows)

	tracker.Complete(part)
	require.NoError(t, tracker.Flush(true))
	require.Empty(t, tracker.parts)
	require.Len(t, memory.updates, 3)
	require.True(t, memory.updates[2][0].Completed)
}
