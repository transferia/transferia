package postgres

import (
	"fmt"

	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/util"
)

const (
	pgLsn = "pg_lsn"
)

var ErrNoKey = xerrors.NewSentinel(fmt.Sprintf("state not contains requested key %s", pgLsn))

type Tracker struct {
	cp         coordinator.Coordinator
	transferID string
}

type LsnState struct {
	SlotID       string `json:"slot_id"`
	CommittedLsn string `json:"commited_lsn"`
}

func (n *Tracker) StoreLsn(slotID, lsn string) error {
	logger.Log.Infof("track lsn %v", lsn)
	return n.cp.SetTransferState(n.transferID, map[string]*coordinator.TransferStateData{
		pgLsn: {
			Generic: &LsnState{
				SlotID:       slotID,
				CommittedLsn: lsn,
			},
		},
	})
}

func (n *Tracker) RemoveLsn() error {
	return n.cp.RemoveTransferState(n.transferID, []string{pgLsn})
}

func (n *Tracker) GetLsn() (*LsnState, error) {
	res, err := n.cp.GetTransferState(n.transferID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get transfer state: %w", err)
	}
	state, ok := res[pgLsn]
	if !ok {
		return nil, ErrNoKey
	}

	if state.GetGeneric() == nil {
		return nil, nil
	}

	var lastLsn LsnState
	if err := util.MapFromJSON(state.Generic, &lastLsn); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal transfer state: %w", err)
	}
	return &lastLsn, nil
}

func NewTracker(transferID string, cp coordinator.Coordinator) *Tracker {
	return &Tracker{
		cp:         cp,
		transferID: transferID,
	}
}
