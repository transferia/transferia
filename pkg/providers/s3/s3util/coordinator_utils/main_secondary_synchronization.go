package coordinator_utils

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
)

func buildStringWorkerDoneKey(num int) string {
	return fmt.Sprintf("worker_done_%d", num)
}

func ResetWorkersDone(cp coordinator.Coordinator, transferID string, runtime abstract.ShardingTaskRuntime) error {
	effectiveMaxWorkerNum := effective_worker_num.DetermineMaxEffectiveWorkerNum(runtime)
	for i := 0; i < effectiveMaxWorkerNum; i++ {
		currKey := buildStringWorkerDoneKey(i)
		state := map[string]*coordinator.TransferStateData{
			currKey: {Generic: "false"},
		}
		err := cp.SetTransferState(transferID, state)
		if err != nil {
			return xerrors.Errorf("unable to set transfer state, err: %w", err)
		}
	}
	return nil
}

func SetWorkerDone(cp coordinator.Coordinator, transferID string, effectiveWorkerNum *effective_worker_num.EffectiveWorkerNum) error {
	currKey := buildStringWorkerDoneKey(effectiveWorkerNum.CurrentWorkerNum)
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
