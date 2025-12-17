package dispatcher

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/synthetic_partition"
	"github.com/transferia/transferia/pkg/util"
)

type DispatcherImmutablePart struct {
	numberOfSyntheticPartitions int // number of synthetic_partitions
	overlapDuration             time.Duration
	effectiveWorkerNum          *effective_worker_num.EffectiveWorkerNum
	mySyntheticPartitions       map[int]*synthetic_partition.SyntheticPartition
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
	_, ok := d.mySyntheticPartitions[syntheticPartitionNum]
	return ok
}

func (d *DispatcherImmutablePart) generateAllSyntheticPartitions() []*synthetic_partition.SyntheticPartition {
	return generateAllSyntheticPartitions(d.numberOfSyntheticPartitions, d.overlapDuration)
}

func (d *DispatcherImmutablePart) generateMySyntheticPartitions() *Task {
	return NewTask(generateMySyntheticPartitions(d.effectiveWorkerNum, d.numberOfSyntheticPartitions, d.overlapDuration))
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
		mySyntheticPartitions:       generateMySyntheticPartitionsMap(inEffectiveWorkerNum, numberOfSyntheticPartitions, overlapDuration),
	}
}
