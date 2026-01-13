package coordinator_utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/providers/s3/sink/testutil"
)

func TestMainSecondarySynchronization(t *testing.T) {
	cp := testutil.NewFakeClientWithTransferState()
	transferID := "dtt"

	runtimeWorker0 := abstract.NewFakeShardingTaskRuntime(0, 2, 1, 0) // main worker
	runtimeWorker1 := abstract.NewFakeShardingTaskRuntime(1, 2, 1, 0)
	runtimeWorker2 := abstract.NewFakeShardingTaskRuntime(2, 2, 1, 0)
	workers := []abstract.ShardingTaskRuntime{runtimeWorker1, runtimeWorker2}

	effectiveWorkerNum1, err := effective_worker_num.NewEffectiveWorkerNum(logger.Log, runtimeWorker1, true)
	require.NoError(t, err)
	effectiveWorkerNum0, err := effective_worker_num.NewEffectiveWorkerNum(logger.Log, runtimeWorker2, true)
	require.NoError(t, err)

	checkPresenceEffectiveWorkers := func(t *testing.T, in map[int]bool) {
		state, err := cp.GetTransferState(transferID)
		require.NoError(t, err)

		for k, v := range in {
			currKey := buildStringWorkerDoneKey(k)
			if v {
				require.NotNil(t, state[currKey])
			} else {
				require.Nil(t, state[currKey])
			}
		}
	}

	checkDone := func(t *testing.T, inWorkers []abstract.ShardingTaskRuntime, expectedNum int, isDone bool) {
		for _, currRuntime := range workers {
			effectiveWorkerNum, err := effective_worker_num.NewEffectiveWorkerNum(logger.Log, currRuntime, true)
			require.NoError(t, err)

			currWorkersDoneCount, err := WorkersDoneCount(time.Now(), cp, transferID, effectiveWorkerNum.WorkersCount)
			require.NoError(t, err)
			require.Equal(t, expectedNum, currWorkersDoneCount)
		}
	}

	// tests

	checkPresenceEffectiveWorkers(t, map[int]bool{0: false, 1: false, 2: false, 3: false}) // checkPresenceEffectiveWorkers in cp absent worker_done_key
	checkDone(t, workers, 0, false)

	err = ResetWorkersDone(cp, transferID, runtimeWorker0)
	require.NoError(t, err)

	checkPresenceEffectiveWorkers(t, map[int]bool{0: true, 1: true, 2: false, 3: false}) // checkPresenceEffectiveWorkers in cp present worker_done_key - for key 0..2
	checkDone(t, workers, 0, false)

	err = SetWorkerDone(cp, transferID, effectiveWorkerNum1) // 1nd (effectiveWorkerNum=#1) worker done
	require.NoError(t, err)

	checkDone(t, workers, 1, false)

	err = SetWorkerDone(cp, transferID, effectiveWorkerNum1) // idempotency
	require.NoError(t, err)

	checkDone(t, workers, 1, false)

	err = SetWorkerDone(cp, transferID, effectiveWorkerNum0) // 2nd (effectiveWorkerNum=#0) worker done
	require.NoError(t, err)

	checkDone(t, workers, 2, true)
}
