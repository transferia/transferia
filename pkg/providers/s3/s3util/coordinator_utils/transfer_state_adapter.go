package coordinator_utils

import (
	"sync"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

type TransferStateAdapter struct {
	// config
	cp                 coordinator.Coordinator
	throttleCPDuration time.Duration
	transferID         string

	// adapter, overrides in tests
	NowGetter func() time.Time

	// state
	mutex        sync.Mutex
	lastPushTime time.Time
	lastState    map[string]*coordinator.TransferStateData
}

// used in:
//   - snapshot - on secondary worker - to handle restarts
//   - replication - to handle restarts
func (a *TransferStateAdapter) GetTransferState() (map[string]*coordinator.TransferStateData, error) {
	stateMap, err := a.cp.GetTransferState(a.transferID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get transfer state: %w", err)
	}
	return stateMap, nil
}

func (a *TransferStateAdapter) SetTransferState(state map[string]*coordinator.TransferStateData) error {
	now := a.NowGetter()
	if now.Sub(a.getLastPushTime()) < a.throttleCPDuration {
		return nil
	}
	a.setLastState(state)
	return a.Flush(now)
}

func (a *TransferStateAdapter) setLastPushTime(inNow time.Time) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.lastPushTime = inNow
}

func (a *TransferStateAdapter) getLastPushTime() time.Time {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	return a.lastPushTime
}

func (a *TransferStateAdapter) setLastState(state map[string]*coordinator.TransferStateData) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.lastState = state
}

func (a *TransferStateAdapter) getLastState() map[string]*coordinator.TransferStateData {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	return a.lastState
}

func (a *TransferStateAdapter) Flush(now time.Time) error {
	currLastState := a.getLastState()
	a.setLastPushTime(now)
	if currLastState == nil {
		return nil
	}
	return a.cp.SetTransferState(a.transferID, currLastState)
}

func NewTransferStateAdapter(cp coordinator.Coordinator, throttleCPDuration time.Duration, transferID string) *TransferStateAdapter {
	return &TransferStateAdapter{
		cp:                 cp,
		throttleCPDuration: throttleCPDuration,
		transferID:         transferID,

		NowGetter: func() time.Time {
			return time.Now()
		},

		mutex:        sync.Mutex{},
		lastPushTime: time.Time{},
		lastState:    nil,
	}
}
