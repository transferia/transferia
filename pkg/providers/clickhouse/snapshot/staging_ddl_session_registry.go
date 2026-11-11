package snapshot

import (
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

// stagingDDLSessionRegistry retains operation state across short-lived sinks.
// Stopping heartbeats retains replica failure history until the operation ends.
type stagingDDLSessionRegistry struct {
	mutex       sync.Mutex
	byOperation map[string]*snapshotOperationState
}

type snapshotOperationState struct {
	keepalives      map[string]*stagingDDLSessionKeepalive
	replicaFailures *ch_eo_common.ReplicaFailures
}

func newStagingDDLSessionRegistry() *stagingDDLSessionRegistry {
	return &stagingDDLSessionRegistry{mutex: sync.Mutex{}, byOperation: make(map[string]*snapshotOperationState)}
}

var stagingDDLSessions = newStagingDDLSessionRegistry()

// StopStagingDDLKeepalives releases the current process's state for one
// snapshot operation after its workers finish. It does not remove snapshot state
// or tables, and is safe to call after DoneShardedTableLoad, Commit, or Cleanup.
func StopStagingDDLKeepalives(transferID string, operationID string) {
	stagingDDLSessions.release(stagingDDLSessionOperationKey(transferID, operationID))
}

func stagingDDLSessionOperationKey(transferID string, operationID string) string {
	return transferID + "\x00" + operationID
}

func (s *stagingDDLSessionRegistry) start(operationKey, sessionID string, create func() (*stagingDDLSessionKeepalive, error)) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	operation := s.byOperation[operationKey]
	if operation != nil && operation.keepalives[sessionID] != nil {
		return nil
	}
	keepalive, err := create()
	if err != nil {
		return xerrors.Errorf("create staging DDL keepalive: %w", err)
	}
	s.operationLocked(operationKey).keepalives[sessionID] = keepalive
	return nil
}

func (s *stagingDDLSessionRegistry) replicaFailures(operationKey string) *ch_eo_common.ReplicaFailures {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	operation := s.operationLocked(operationKey)
	if operation.replicaFailures == nil {
		operation.replicaFailures = new(ch_eo_common.ReplicaFailures)
	}
	return operation.replicaFailures
}

// operationLocked requires the registry mutex.
func (s *stagingDDLSessionRegistry) operationLocked(operationKey string) *snapshotOperationState {
	operation := s.byOperation[operationKey]
	if operation == nil {
		operation = &snapshotOperationState{
			keepalives:      make(map[string]*stagingDDLSessionKeepalive),
			replicaFailures: nil,
		}
		s.byOperation[operationKey] = operation
	}
	return operation
}

func (s *stagingDDLSessionRegistry) stop(operationKey string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.stopLocked(operationKey)
}

func (s *stagingDDLSessionRegistry) release(operationKey string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.stopLocked(operationKey)
	delete(s.byOperation, operationKey)
}

// stopLocked requires the registry mutex.
func (s *stagingDDLSessionRegistry) stopLocked(operationKey string) {
	operation := s.byOperation[operationKey]
	if operation == nil {
		return
	}
	// Serialize stop and start: a replacement must not overlap a canceled probe.
	for _, keepalive := range operation.keepalives {
		keepalive.stopAndWait()
	}
	clear(operation.keepalives)
	if operation.replicaFailures == nil {
		delete(s.byOperation, operationKey)
	}
}
