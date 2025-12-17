package dispatcher

import (
	"time"

	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/synthetic_partition"
	"github.com/transferia/transferia/pkg/util/slicesx"
)

func generateAllSyntheticPartitions(numberOfSyntheticPartitions int, overlapDuration time.Duration) []*synthetic_partition.SyntheticPartition {
	result := make([]*synthetic_partition.SyntheticPartition, 0, numberOfSyntheticPartitions)
	for i := 0; i < numberOfSyntheticPartitions; i++ {
		result = append(result, synthetic_partition.NewSyntheticPartition(i, overlapDuration))
	}
	return result
}

func generateMySyntheticPartitions(effectiveWorkerNum *effective_worker_num.EffectiveWorkerNum, numberOfSyntheticPartitions int, overlapDuration time.Duration) []*synthetic_partition.SyntheticPartition {
	allSyntheticPartitions := generateAllSyntheticPartitions(numberOfSyntheticPartitions, overlapDuration)
	allTasks := slicesx.SplitToChunks(allSyntheticPartitions, effectiveWorkerNum.WorkersCount)
	return allTasks[effectiveWorkerNum.CurrentWorkerNum]
}

func generateMySyntheticPartitionsMap(effectiveWorkerNum *effective_worker_num.EffectiveWorkerNum, numberOfSyntheticPartitions int, overlapDuration time.Duration) map[int]*synthetic_partition.SyntheticPartition {
	mySyntheticPartitions := generateMySyntheticPartitions(effectiveWorkerNum, numberOfSyntheticPartitions, overlapDuration)

	result := make(map[int]*synthetic_partition.SyntheticPartition)
	for _, currSyntheticPartition := range mySyntheticPartitions {
		result[currSyntheticPartition.SyntheticPartitionNum()] = currSyntheticPartition
	}
	return result
}
