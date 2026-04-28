package logtracker

import (
	"sync"

	oracle_common "github.com/transferia/transferia/pkg/providers/oracle/common"
)

// inMemoryStore is a process-wide map so that positions written by one
// InMemoryLogTracker instance (e.g. during Activate) are visible to another
// instance created later for the same transferID (e.g. during replication).
var (
	inMemoryStore   = make(map[string]*oracle_common.LogPosition)
	inMemoryStoreMu sync.Mutex
)

type InMemoryLogTracker struct {
	transferID string
}

func NewInMemoryLogTracker(transferID string) (*InMemoryLogTracker, error) {
	return &InMemoryLogTracker{transferID: transferID}, nil
}

func (tracker *InMemoryLogTracker) TransferID() string {
	return tracker.transferID
}

func (tracker *InMemoryLogTracker) Init() error {
	return nil
}

func (tracker *InMemoryLogTracker) ClearPosition() error {
	inMemoryStoreMu.Lock()
	defer inMemoryStoreMu.Unlock()
	delete(inMemoryStore, tracker.transferID)
	return nil
}

func (tracker *InMemoryLogTracker) ReadPosition() (*oracle_common.LogPosition, error) {
	inMemoryStoreMu.Lock()
	defer inMemoryStoreMu.Unlock()
	return inMemoryStore[tracker.transferID], nil
}

func (tracker *InMemoryLogTracker) WritePosition(position *oracle_common.LogPosition) error {
	inMemoryStoreMu.Lock()
	defer inMemoryStoreMu.Unlock()
	inMemoryStore[tracker.transferID] = position
	return nil
}
