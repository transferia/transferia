package tasks

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
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
