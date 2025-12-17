package synthetic_partition

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
)

func TestSyntheticPartitionStatelessPart(t *testing.T) {
	syntheticPartition := NewSyntheticPartition(33, time.Minute)
	require.Equal(t, 33, syntheticPartition.SyntheticPartitionNum())
	require.Equal(t, "33", syntheticPartition.SyntheticPartitionStr())
}

func TestSyntheticPartitionStatefulPart(t *testing.T) {
	// init implicit
	syntheticPartition := NewSyntheticPartition(33, 0)
	require.Equal(t, 33, syntheticPartition.SyntheticPartitionNum())
	require.Equal(t, 0, syntheticPartition.queueToHandle.Size())

	nsecToTime := func(nsec int64) time.Time {
		return time.Unix(0, nsec)
	}

	t.Run("add 'file' with ns=4", func(t *testing.T) {
		require.Equal(t, 0, syntheticPartition.queueToHandle.Size())
		q, err := syntheticPartition.AddIfNew(file.NewFile("file2", 1, nsecToTime(4)))
		require.NoError(t, err)
		require.True(t, q)
		require.Equal(t, 1, syntheticPartition.queueToHandle.Size())
	})

	t.Run("add 'file' with ns=5", func(t *testing.T) {
		require.Equal(t, 1, syntheticPartition.queueToHandle.Size())
		q, err := syntheticPartition.AddIfNew(file.NewFile("file3", 1, nsecToTime(5)))
		require.NoError(t, err)
		require.True(t, q)
		require.Equal(t, 2, syntheticPartition.queueToHandle.Size())
	})
}

func TestSyntheticPartitionCommitOrder(t *testing.T) {
	syntheticPartition := NewSyntheticPartition(33, time.Minute)

	makeFile := func(in string) *file.File {
		nsecToTime := func(nsec int64) time.Time {
			return time.Unix(0, nsec)
		}
		ns, err := strconv.Atoi(string(in[0]))
		require.NoError(t, err)
		return file.NewFile(in, int64(ns), nsecToTime(int64(ns)))
	}

	_, _ = syntheticPartition.AddIfNew(makeFile("1a"))
	_, _ = syntheticPartition.AddIfNew(makeFile("2a"))
	_, _ = syntheticPartition.AddIfNew(makeFile("2b"))
	_, _ = syntheticPartition.AddIfNew(makeFile("3a"))
	_, _ = syntheticPartition.AddIfNew(makeFile("4a"))
	require.Equal(t, 5, syntheticPartition.queueToHandle.TotalSize())

	check := func(syntheticPartition *SyntheticPartition, commitFileName string, afterProgressShouldBe int) {
		oldLen := syntheticPartition.queueToHandle.TotalSize()
		err := syntheticPartition.Commit(commitFileName)
		require.NoError(t, err)
		newLen := syntheticPartition.queueToHandle.TotalSize()
		require.Equal(t, 1, oldLen-newLen)
	}

	check(syntheticPartition, "2b", 0)
	check(syntheticPartition, "3a", 0)
	check(syntheticPartition, "1a", 2)
	check(syntheticPartition, "2a", 3)
}
