package dispatcher

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/synthetic_partition"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/set"
)

type DispatcherImmutablePart struct {
	numberOfSyntheticPartitions int // number of synthetic_partitions
	overlapDuration             time.Duration
	effectiveWorkerNum          *effective_worker_num.EffectiveWorkerNum
	mySyntheticPartitionsNums   *set.Set[int]
}

func (d *DispatcherImmutablePart) DetermineSyntheticPartitionNum(fileName string) int {
	return int(util.CRC32FromString(fileName)) % d.numberOfSyntheticPartitions
}

func (d *DispatcherImmutablePart) DetermineSyntheticPartitionStr(fileName string) string {
	return fmt.Sprintf("%d", d.DetermineSyntheticPartitionNum(fileName))
}

func (d *DispatcherImmutablePart) IsMyFileName(fileName string) bool {
	syntheticPartitionNum := d.DetermineSyntheticPartitionNum(fileName)
	return d.Contains(syntheticPartitionNum)
}

func (d *DispatcherImmutablePart) Contains(syntheticPartitionNum int) bool {
	return d.mySyntheticPartitionsNums.Contains(syntheticPartitionNum)
}

func (d *DispatcherImmutablePart) generateMySyntheticPartitions() *Task {
	nums := d.mySyntheticPartitionsNums.SortedSliceFunc(func(a, b int) bool { return a < b })
	mySyntheticPartitions := make([]*synthetic_partition.SyntheticPartition, 0, len(nums))
	for _, num := range nums {
		mySyntheticPartitions = append(mySyntheticPartitions, synthetic_partition.NewSyntheticPartition(num, d.overlapDuration))
	}
	return NewTask(mySyntheticPartitions)
}

func NewDispatcherImmutablePart(
	numberOfSyntheticPartitions int,
	inEffectiveWorkerNum *effective_worker_num.EffectiveWorkerNum,
	overlapDuration time.Duration,
) *DispatcherImmutablePart {
	return &DispatcherImmutablePart{
		numberOfSyntheticPartitions: numberOfSyntheticPartitions,
		overlapDuration:             overlapDuration,
		effectiveWorkerNum:          inEffectiveWorkerNum,
		mySyntheticPartitionsNums:   generateMySyntheticPartitionsSet(inEffectiveWorkerNum, numberOfSyntheticPartitions, overlapDuration),
	}
}
