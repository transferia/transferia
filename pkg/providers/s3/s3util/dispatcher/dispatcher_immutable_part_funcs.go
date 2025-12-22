package dispatcher

import (
	"time"

	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/util/set"
	"github.com/transferia/transferia/pkg/util/slicesx"
)

func generateAllSyntheticPartitionsNums(numberOfSyntheticPartitions int, overlapDuration time.Duration) []int {
	result := make([]int, 0, numberOfSyntheticPartitions)
	for i := 0; i < numberOfSyntheticPartitions; i++ {
		result = append(result, i)
	}
	return result
}

func generateMySyntheticPartitionsNums(effectiveWorkerNum *effective_worker_num.EffectiveWorkerNum, numberOfSyntheticPartitions int, overlapDuration time.Duration) []int {
	allSyntheticPartitionsNums := generateAllSyntheticPartitionsNums(numberOfSyntheticPartitions, overlapDuration)
	allTasks := slicesx.SplitToChunks(allSyntheticPartitionsNums, effectiveWorkerNum.WorkersCount)
	return allTasks[effectiveWorkerNum.CurrentWorkerNum]
}

func generateMySyntheticPartitionsSet(effectiveWorkerNum *effective_worker_num.EffectiveWorkerNum, numberOfSyntheticPartitions int, overlapDuration time.Duration) *set.Set[int] {
	mySyntheticPartitions := generateMySyntheticPartitionsNums(effectiveWorkerNum, numberOfSyntheticPartitions, overlapDuration)

	result := set.New[int]()
	for _, currSyntheticPartition := range mySyntheticPartitions {
		result.Add(currSyntheticPartition)
	}
	return result
}
