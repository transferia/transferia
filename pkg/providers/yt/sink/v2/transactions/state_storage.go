//go:build !disable_yt_provider

package transactions

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/yt"
)

var (
	SinkYtState     = "sink_yt_state"
	errorEmptyState = xerrors.New("empty yt state")
)

type ytState struct {
	TxID string `json:"tx_id"`
}

type ytStateStorage struct {
	cp         coordinator.Coordinator
	transferID string
	logger     log.Logger
}

func (s *ytStateStorage) GetState() (*yt.TxID, error) {
	id, err := s.getState()
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errorEmptyState
	}

	txID, err := newTxID(id)
	if err != nil {
		return nil, xerrors.Errorf("unable to convert state to TxID: %w", err)
	}
	s.logger.Info("got mainTx from state", log.Any("tx", txID))

	return &txID, nil
}

func (s *ytStateStorage) SetState(tx yt.TxID) error {
	if err := s.cp.SetTransferState(s.transferID, map[string]*coordinator.TransferStateData{
		SinkYtState: {Generic: ytState{TxID: tx.String()}},
	}); err != nil {
		return xerrors.Errorf("unable to store static YT sink state: %w", err)
	}
	s.logger.Info("upload mainTx in state", log.Any("state", tx.String()))

	return nil
}

// RemoveState removes state and return deleted tx id
func (s *ytStateStorage) RemoveState() (*yt.TxID, error) {
	id, err := s.getState()
	if err != nil {
		return nil, xerrors.Errorf("unable to check previous static YT sink state: %w", err)
	}

	var prevTxID *yt.TxID
	if id != "" {
		txID, err := newTxID(id)
		if err != nil {
			return nil, xerrors.Errorf("unable to convert previous state to TxID, state: %s: %w", id, err)
		}
		prevTxID = &txID
	}

	if err := s.cp.RemoveTransferState(s.transferID, []string{SinkYtState}); err != nil {
		return nil, err
	}

	return prevTxID, nil
}

func (s *ytStateStorage) getState() (string, error) {
	var res ytState

	if err := backoff.RetryNotify(
		func() error {
			stateMsg, err := s.cp.GetTransferState(s.transferID)
			if err != nil {
				return xerrors.Errorf("failed to get operation sink state: %w", err)
			}
			if state, ok := stateMsg[SinkYtState]; ok && state != nil && state.GetGeneric() != nil {
				if err := util.MapFromJSON(state.Generic, &res); err != nil {
					return xerrors.Errorf("unable to unmarshal state: %w", err)
				}
			}
			return nil
		},
		backoff.WithMaxRetries(backoff.NewExponentialBackOff(), maxRetriesCount),
		util.BackoffLoggerDebug(s.logger, "waiting for sharded sink state"),
	); err != nil {
		return "", xerrors.Errorf("failed while waiting for sharded sink state: %w", err)
	}
	return res.TxID, nil
}

func newTxID(id string) (yt.TxID, error) {
	txID, err := guid.ParseString(id)
	if err != nil {
		return yt.TxID{}, err
	}

	return yt.TxID(txID), nil
}

func newYtStateStorage(cp coordinator.Coordinator, transferID string, logger log.Logger) *ytStateStorage {
	return &ytStateStorage{
		cp:         cp,
		transferID: transferID,
		logger:     logger,
	}
}
