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

	dispatcher := NewDispatcherImmutablePart(5, effectiveWorkerNum, 0)
	require.Equal(t, 5, dispatcher.numberOfSyntheticPartitions)

	all := dispatcher.generateAllSyntheticPartitions()
	require.Equal(t, 5, len(all))
	for i := 0; i < 5; i++ {
		require.Equal(t, i, all[i].SyntheticPartitionNum())
	}

	myTask := dispatcher.generateMySyntheticPartitions()
	require.Equal(t, 3, len(myTask.mySyntheticPartitions))
	require.Equal(t, 0, myTask.mySyntheticPartitions[0].SyntheticPartitionNum())
	require.Equal(t, 1, myTask.mySyntheticPartitions[1].SyntheticPartitionNum())
	require.Equal(t, 2, myTask.mySyntheticPartitions[2].SyntheticPartitionNum())
}
