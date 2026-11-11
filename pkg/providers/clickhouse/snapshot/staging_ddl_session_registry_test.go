package snapshot

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestStagingKeepalive(released *atomic.Int32) *stagingDDLSessionKeepalive {
	ctx, cancel := context.WithCancel(context.Background())
	keepalive := &stagingDDLSessionKeepalive{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(keepalive.done)
		<-ctx.Done()
		released.Add(1)
	}()
	return keepalive
}

func TestStagingDDLRegistryConcurrentStartAndStop(test *testing.T) {
	registry := newStagingDDLSessionRegistry()
	var created atomic.Int32
	var released atomic.Int32
	var workers sync.WaitGroup
	for workerIndex := 0; workerIndex < 32; workerIndex++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			err := registry.start("transfer/operation", "session", func() (*stagingDDLSessionKeepalive, error) {
				created.Add(1)
				return newTestStagingKeepalive(&released), nil
			})
			if err != nil {
				test.Errorf("start keepalive: %v", err)
			}
		}()
	}
	workers.Wait()
	require.EqualValues(test, 1, created.Load())
	registry.stop("transfer/operation")
	require.EqualValues(test, 1, released.Load())
	require.Empty(test, registry.byOperation)
	registry.stop("transfer/operation")
	require.EqualValues(test, 1, released.Load())
}

func TestStagingDDLRegistryOperationIsolation(test *testing.T) {
	registry := newStagingDDLSessionRegistry()
	var firstReleased atomic.Int32
	var secondReleased atomic.Int32
	require.NoError(test, registry.start("first", "session", func() (*stagingDDLSessionKeepalive, error) {
		return newTestStagingKeepalive(&firstReleased), nil
	}))
	require.NoError(test, registry.start("second", "session", func() (*stagingDDLSessionKeepalive, error) {
		return newTestStagingKeepalive(&secondReleased), nil
	}))
	registry.stop("first")
	require.EqualValues(test, 1, firstReleased.Load())
	require.Zero(test, secondReleased.Load())
	require.Len(test, registry.byOperation, 1)
	registry.stop("second")
	require.EqualValues(test, 1, secondReleased.Load())
	require.Empty(test, registry.byOperation)
}

func TestStagingDDLRegistryFailedCreationCanBeRetried(test *testing.T) {
	registry := newStagingDDLSessionRegistry()
	expectedError := errors.New("transport unavailable")
	err := registry.start("operation", "session", func() (*stagingDDLSessionKeepalive, error) {
		return nil, expectedError
	})
	require.ErrorIs(test, err, expectedError)
	require.Empty(test, registry.byOperation)
	var released atomic.Int32
	require.NoError(test, registry.start("operation", "session", func() (*stagingDDLSessionKeepalive, error) {
		return newTestStagingKeepalive(&released), nil
	}))
	registry.stop("operation")
	require.EqualValues(test, 1, released.Load())
}

func TestStopStagingDDLKeepalivesOperationIsolation(test *testing.T) {
	transferID := test.Name()
	otherTransferID := transferID + "-other"
	const operationID = "current"
	const otherOperationID = "other"
	var currentReleased atomic.Int32
	var otherOperationReleased atomic.Int32
	var otherTransferReleased atomic.Int32
	test.Cleanup(func() {
		StopStagingDDLKeepalives(transferID, operationID)
		StopStagingDDLKeepalives(transferID, otherOperationID)
		StopStagingDDLKeepalives(otherTransferID, operationID)
	})
	require.NoError(test, stagingDDLSessions.start(stagingDDLSessionOperationKey(transferID, operationID), "session", func() (*stagingDDLSessionKeepalive, error) {
		return newTestStagingKeepalive(&currentReleased), nil
	}))
	require.NoError(test, stagingDDLSessions.start(stagingDDLSessionOperationKey(transferID, otherOperationID), "session", func() (*stagingDDLSessionKeepalive, error) {
		return newTestStagingKeepalive(&otherOperationReleased), nil
	}))
	require.NoError(test, stagingDDLSessions.start(stagingDDLSessionOperationKey(otherTransferID, operationID), "session", func() (*stagingDDLSessionKeepalive, error) {
		return newTestStagingKeepalive(&otherTransferReleased), nil
	}))

	StopStagingDDLKeepalives(transferID, operationID)
	StopStagingDDLKeepalives(transferID, operationID)

	require.EqualValues(test, 1, currentReleased.Load())
	require.Zero(test, otherOperationReleased.Load())
	require.Zero(test, otherTransferReleased.Load())
}

func TestReplicaFailureHistoryLivesUntilOperationClose(test *testing.T) {
	transferID := test.Name()
	const operationID = "operation"
	operationKey := stagingDDLSessionOperationKey(transferID, operationID)
	test.Cleanup(func() { StopStagingDDLKeepalives(transferID, operationID) })

	failures := stagingDDLSessions.replicaFailures(operationKey)
	stagingDDLSessions.stop(operationKey)
	require.Same(test, failures, stagingDDLSessions.replicaFailures(operationKey))

	StopStagingDDLKeepalives(transferID, operationID)
	require.NotSame(test, failures, stagingDDLSessions.replicaFailures(operationKey))
}
