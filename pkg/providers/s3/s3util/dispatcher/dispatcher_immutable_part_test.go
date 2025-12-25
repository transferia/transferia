package dispatcher

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
)

func TestDispatcherStatelessPart(t *testing.T) {
	parallelism := abstract.NewFakeShardingTaskRuntime(0, 999, 777, 2)
	effectiveWorkerNum, err := effective_worker_num.NewEffectiveWorkerNum(logger.Log, parallelism, false)
	require.NoError(t, err)

	t.Run("default test", func(t *testing.T) {
		dispatcher := NewDispatcherImmutablePart(5, effectiveWorkerNum, 0)
		require.Equal(t, 5, dispatcher.numberOfSyntheticPartitions)

		myTask := dispatcher.generateMySyntheticPartitions()
		require.Len(t, myTask.mySyntheticPartitions, 3)
		require.Equal(t, 0, myTask.mySyntheticPartitions[0].SyntheticPartitionNum())
		require.Equal(t, 1, myTask.mySyntheticPartitions[1].SyntheticPartitionNum())
		require.Equal(t, 2, myTask.mySyntheticPartitions[2].SyntheticPartitionNum())
	})

	t.Run("determine synthetic partition num", func(t *testing.T) {
		numberOfSyntheticPartitions := 100000

		dispatcher := NewDispatcherImmutablePart(numberOfSyntheticPartitions, effectiveWorkerNum, 0)
		require.Equal(t, numberOfSyntheticPartitions, dispatcher.numberOfSyntheticPartitions)

		myTask := dispatcher.generateMySyntheticPartitions()
		chunkSize := (numberOfSyntheticPartitions + effectiveWorkerNum.WorkersCount - 1) / effectiveWorkerNum.WorkersCount
		startPartition := effectiveWorkerNum.CurrentWorkerNum * chunkSize
		endPartition := startPartition + chunkSize
		if endPartition > numberOfSyntheticPartitions {
			endPartition = numberOfSyntheticPartitions
		}
		expectedPartitionsCount := endPartition - startPartition

		require.Len(t, myTask.mySyntheticPartitions, expectedPartitionsCount)
		for i := 0; i < len(myTask.mySyntheticPartitions); i++ {
			require.Equal(t, startPartition+i, myTask.mySyntheticPartitions[i].SyntheticPartitionNum())
		}
	})
}
