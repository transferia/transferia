package coordinator_utils

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/dispatcher"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/lr_window/r_window"
	"go.ytsaurus.tech/library/go/core/log"
)

func buildStringWorkerDoneKey(num int) string {
	return fmt.Sprintf("worker_done_%d", num)
}

func ResetTransferState(lgr log.Logger, cp coordinator.Coordinator, transferID string, runtime abstract.ShardingTaskRuntime, syntheticPartitionsNum int) error {
	currDispatcher := dispatcher.NewDispatcher(
		r_window.NewRWindowEmpty(time.Duration(0)),
		syntheticPartitionsNum,
		effective_worker_num.NewEffectiveWorkerNumSingleWorker(),
		time.Duration(0),
	)
	state := currDispatcher.SerializeState()

	stateBytes, err := json.Marshal(state)
	if err != nil {
		return xerrors.Errorf("Failed to serialize state: %w", err)
	}
	lgr.Infof("will reset transfer state: %s", string(stateBytes))

	err = cp.SetTransferState(transferID, state)
	if err != nil {
		return xerrors.Errorf("unable to set transfer state, err: %w", err)
	}
	return nil
}

func ResetWorkersDone(lgr log.Logger, cp coordinator.Coordinator, transferID string, runtime abstract.ShardingTaskRuntime) error {
	effectiveMaxWorkerNum := effective_worker_num.DetermineMaxEffectiveWorkerNum(runtime)
	lgr.Infof("will reset workers done for %d workers", effectiveMaxWorkerNum)
	keys := make([]string, 0)
	for i := 0; i < effectiveMaxWorkerNum; i++ {
		currKey := buildStringWorkerDoneKey(i)
		keys = append(keys, currKey)
		state := map[string]*coordinator.TransferStateData{
			currKey: {Generic: "false"},
		}
		err := cp.SetTransferState(transferID, state)
		if err != nil {
			return xerrors.Errorf("unable to set transfer state, err: %w", err)
		}
	}
	lgr.Infof("reset workers done: %v", keys)
	return nil
}

func SetWorkerDone(lgr log.Logger, cp coordinator.Coordinator, transferID string, effectiveWorkerNum *effective_worker_num.EffectiveWorkerNum) error {
	currKey := buildStringWorkerDoneKey(effectiveWorkerNum.CurrentWorkerNum)
	lgr.Infof("will set workers done, currKey=%s", currKey)
	state := map[string]*coordinator.TransferStateData{
		currKey: {Generic: "true"},
	}
	err := cp.SetTransferState(transferID, state)
	if err != nil {
		return xerrors.Errorf("unable to set transfer state, err: %w", err)
	}
	return nil
}

func WorkersDoneCount(startTime time.Time, cp coordinator.Coordinator, transferID string, maxWorkerNum int) (int, error) {
	state, err := cp.GetTransferState(transferID)
	if err != nil {
		return 0, xerrors.Errorf("unable to get transfer state, err: %w", err)
	}
	counter := 0
	for i := 0; i < maxWorkerNum; i++ {
		currKey := buildStringWorkerDoneKey(i)
		v := state[currKey]
		if v == nil {
			continue
		}
		if v.Generic == "true" {
			counter++
		}
	}
	return counter, nil
}
