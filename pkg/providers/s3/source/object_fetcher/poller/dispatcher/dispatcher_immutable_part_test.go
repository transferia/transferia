package dispatcher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDispatcherStatlessPart(t *testing.T) {
	workerProperties, err := NewWorkerProperties(0, 2)
	require.NoError(t, err)

	dispatcher := NewDispatcherImmutablePart(5, workerProperties)
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
