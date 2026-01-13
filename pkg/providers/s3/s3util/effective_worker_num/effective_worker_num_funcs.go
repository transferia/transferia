package effective_worker_num

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// the main idea:
//   - for example, we have 3 worker - their numbers: 0,1,2
//   - 0 worker is 'main' worker
//   - then this function says: "we have 2 worker with nums: 0 & 1"
//   - worker '0' is impossible
//   - worker '1' worker worker '1'
//   - worker '2' became worker '0'
func determineCurrentAndMaxEffectiveWorkerNum(snapshotRuntime abstract.ShardingTaskRuntime) (int, int, error) {
	currentJobIndex := snapshotRuntime.CurrentJobIndex()
	secondaryWorkersCount := snapshotRuntime.SnapshotWorkersNum()

	if currentJobIndex == 0 {
		return 0, 0, xerrors.New("impossible situation - seconary worker got workerNum==0")
	}

	if currentJobIndex == secondaryWorkersCount {
		return 0, DetermineMaxEffectiveWorkerNum(snapshotRuntime), nil
	} else {
		return currentJobIndex, DetermineMaxEffectiveWorkerNum(snapshotRuntime), nil
	}
}

func DetermineMaxEffectiveWorkerNum(snapshotRuntime abstract.ShardingTaskRuntime) int {
	return snapshotRuntime.SnapshotWorkersNum()
}
