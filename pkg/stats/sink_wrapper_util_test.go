package stats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
)

func TestBatchStats(t *testing.T) {
	toNS := func(nsec int64) uint64 {
		return uint64(time.Unix(0, nsec).UnixNano())
	}

	t.Run("default case", func(t *testing.T) {
		oldestTime, freshestTime, rowEvents, bytes := batchStats(logger.Log, []abstract.ChangeItem{
			{Kind: abstract.InsertKind, CommitTime: toNS(19), Table: "my_table", Size: changeitem.EventSize{Read: 1, Values: 1}},
			{Kind: abstract.InsertKind, CommitTime: toNS(99), Table: "my_table", Size: changeitem.EventSize{Read: 1, Values: 2}},
		})
		require.Equal(t, int64(99), oldestTime.UnixNano())
		require.Equal(t, int64(19), freshestTime.UnixNano())
		require.Equal(t, int64(2), rowEvents)
		require.Equal(t, uint64(0x3), bytes)
	})

	t.Run("SynchronizeEvent", func(t *testing.T) {
		now := time.Now()
		oldestTime, freshestTime, rowEvents, bytes := batchStats(logger.Log, []abstract.ChangeItem{
			abstract.MakeSynchronizeEvent(),
		})
		oldestDiff := oldestTime.Sub(now)
		freshestDiff := freshestTime.Sub(now)

		require.True(t, oldestDiff < time.Second)
		require.True(t, freshestDiff < time.Second)
		require.Equal(t, int64(0), rowEvents)
		require.Equal(t, uint64(0x0), bytes)
	})
}
