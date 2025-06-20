//go:build !disable_postgres_provider

package sequencer

import (
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// Sequencer takes items and updates progressInfo
type Sequencer struct {
	mutex    sync.Mutex
	progress *progressInfo
}

// StartProcessing receives changes in correct transaction order because we read from server synchronously
func (s *Sequencer) StartProcessing(changes []abstract.ChangeItem) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, item := range changes {
		if err := s.progress.add(item.ID, item.LSN); err != nil {
			return xerrors.Errorf("failed to start processing: %w", err)
		}
	}
	return nil
}

func (s *Sequencer) Pushed(changes []abstract.ChangeItem) (uint64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	transactionsToLsns := make(map[uint32][]uint64)

	for _, item := range changes {
		if !s.progress.check(item.ID) {
			return 0, xerrors.Errorf("transaction %v is not found in processed transactions", item.ID)
		}
		if _, ok := transactionsToLsns[item.ID]; !ok {
			transactionsToLsns[item.ID] = make([]uint64, 0)
		}
		transactionsToLsns[item.ID] = append(transactionsToLsns[item.ID], item.LSN)
	}

	for id, pushed := range transactionsToLsns {
		if err := s.progress.remove(id, pushed); err != nil {
			return 0, xerrors.Errorf("unable to remove pushed items from sequencer: %w", err)
		}
	}
	return s.progress.updateCommitted(), nil
}

func NewSequencer() *Sequencer {
	return &Sequencer{
		mutex:    sync.Mutex{},
		progress: newProgressInfo(),
	}
}
