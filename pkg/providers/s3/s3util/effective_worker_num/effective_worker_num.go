package effective_worker_num

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

type EffectiveWorkerNum struct {
	CurrentWorkerNum int
	WorkersCount     int
}

func NewEffectiveWorkerNumSingleWorker() *EffectiveWorkerNum {
	return &EffectiveWorkerNum{
		CurrentWorkerNum: 0,
		WorkersCount:     1,
	}
}

func NewEffectiveWorkerNum(logger log.Logger, inRuntime abstract.ShardingTaskRuntime, isSnapshot bool) (*EffectiveWorkerNum, error) {
	var result *EffectiveWorkerNum = nil
	if isSnapshot {
		currentWorkerNum, maxWorkerNum, err := determineCurrentAndMaxEffectiveWorkerNum(inRuntime)
		if err != nil {
			return nil, xerrors.Errorf("failed to determine current and max worker num, err: %w", err)
		}
		result = &EffectiveWorkerNum{
			CurrentWorkerNum: currentWorkerNum,
			WorkersCount:     maxWorkerNum,
		}
		logger.Infof("derived [snapshot_mode] (current,max): (%d/%d) -> (%d/%d)", inRuntime.CurrentJobIndex(), inRuntime.SnapshotWorkersNum(), result.CurrentWorkerNum, result.WorkersCount)
	} else {
		result = &EffectiveWorkerNum{
			CurrentWorkerNum: inRuntime.CurrentJobIndex(),
			WorkersCount:     inRuntime.ReplicationWorkersNum(),
		}
		logger.Infof("derived [replication_mode] (current,max): (%d/%d) -> (%d/%d)", inRuntime.CurrentJobIndex(), inRuntime.ReplicationWorkersNum(), result.CurrentWorkerNum, result.WorkersCount)
	}
	if result.CurrentWorkerNum >= result.WorkersCount {
		return nil, xerrors.Errorf("NewEffectiveWorkerNum: result.CurrentWorkerNum >= result.WorkersCount, result.CurrentWorkerNum:%d, result.WorkersCount:%d", result.CurrentWorkerNum, result.WorkersCount)
	}
	return result, nil
}
