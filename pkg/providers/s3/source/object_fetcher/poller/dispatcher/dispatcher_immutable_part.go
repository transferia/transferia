package dispatcher

import (
	"fmt"

	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/synthetic_partition"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/slicesx"
)

type DispatcherImmutablePart struct {
	numberOfSyntheticPartitions int // number of synthetic_partitions
	myWorkerProperties          *WorkerProperties
	myTask                      *Task
}

func (d *DispatcherImmutablePart) DetermineSyntheticPartitionNum(fileName string) int {
	return int(util.CRC32FromString(fileName)) % d.numberOfSyntheticPartitions
}

func (d *DispatcherImmutablePart) DetermineSyntheticPartitionStr(fileName string) string {
	return fmt.Sprintf("%d", d.DetermineSyntheticPartitionNum(fileName))
}

func (d *DispatcherImmutablePart) IsMyFileName(fileName string) bool {
	syntheticPartitionNum := d.DetermineSyntheticPartitionNum(fileName)
	return d.myTask.Contains(syntheticPartitionNum)
}

func (d *DispatcherImmutablePart) generateAllSyntheticPartitions() []*synthetic_partition.SyntheticPartition {
	result := make([]*synthetic_partition.SyntheticPartition, 0, d.numberOfSyntheticPartitions)
	for i := 0; i < d.numberOfSyntheticPartitions; i++ {
		result = append(result, synthetic_partition.NewSyntheticPartition(i))
	}
	return result
}

func (d *DispatcherImmutablePart) generateMySyntheticPartitions() *Task {
	allSyntheticPartitions := d.generateAllSyntheticPartitions()
	allTasks := slicesx.SplitToChunks(allSyntheticPartitions, d.myWorkerProperties.totalWorkersNum)
	mySyntheticPartitions := allTasks[d.myWorkerProperties.currentWorkerNum]
	myTask := NewTask(mySyntheticPartitions)
	return myTask
}

func NewDispatcherImmutablePart(numberOfSyntheticPartitions int, myWorkerProperties *WorkerProperties) *DispatcherImmutablePart {
	result := &DispatcherImmutablePart{
		numberOfSyntheticPartitions: numberOfSyntheticPartitions,
		myWorkerProperties:          myWorkerProperties,
		myTask:                      nil,
	}
	myTask := result.generateMySyntheticPartitions()
	result.myTask = myTask
	return result
}
