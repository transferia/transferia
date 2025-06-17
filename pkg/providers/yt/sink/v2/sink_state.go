//go:build !disable_yt_provider

package staticsink

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
)

var SinkYtState = "static_dynamic_sink_yt_state"

type ytState struct {
	Tables []ypath.Path `json:"tx_id"`
}

type ytStateStorage struct {
	cp         coordinator.Coordinator
	transferID string
	logger     log.Logger
}

func (s *ytStateStorage) GetState() (*ytState, error) {
	state, err := s.getState()
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, xerrors.Errorf("state was empty")
	}
	if len(state.Tables) > 0 {
		s.logger.Info("got tables from state", log.Any("tables", state.Tables))
	}

	return state, nil
}

func (s *ytStateStorage) SetState(tables []ypath.Path) error {
	if err := s.cp.SetTransferState(s.transferID, map[string]*coordinator.TransferStateData{
		SinkYtState: {Generic: ytState{Tables: tables}},
	}); err != nil {
		return xerrors.Errorf("unable to store static YT sink state: %w", err)
	}
	s.logger.Info("upload tables in state", log.Any("tables", tables))

	return nil
}

func (s *ytStateStorage) RemoveState() error {
	if err := s.cp.RemoveTransferState(s.transferID, []string{SinkYtState}); err != nil {
		return err
	}

	return nil
}

func (s *ytStateStorage) getState() (*ytState, error) {
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
		backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5),
		util.BackoffLoggerDebug(s.logger, "waiting for sharded sink state"),
	); err != nil {
		return nil, xerrors.Errorf("failed while waiting for sharded sink state: %w", err)
	}
	return &res, nil
}

func newYtStateStorage(cp coordinator.Coordinator, transferID string, logger log.Logger) *ytStateStorage {
	return &ytStateStorage{
		cp:         cp,
		transferID: transferID,
		logger:     logger,
	}
}
