package effective_worker_num

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
)

func TestReplication(t *testing.T) {
	t.Run("", func(t *testing.T) {
		parallelism := abstract.NewFakeShardingTaskRuntime(0, 999, 777, 2)
		effectiveWorkerNum, err := NewEffectiveWorkerNum(logger.Log, parallelism, false)
		require.NoError(t, err)
		require.Equal(t, 0, effectiveWorkerNum.CurrentWorkerNum)
		require.Equal(t, 2, effectiveWorkerNum.WorkersCount)
	})

	t.Run("", func(t *testing.T) {
		parallelism := abstract.NewFakeShardingTaskRuntime(1, 999, 777, 2)
		effectiveWorkerNum, err := NewEffectiveWorkerNum(logger.Log, parallelism, false)
		require.NoError(t, err)
		require.Equal(t, 1, effectiveWorkerNum.CurrentWorkerNum)
		require.Equal(t, 2, effectiveWorkerNum.WorkersCount)
	})
}

func TestSnapshot(t *testing.T) {
	t.Run("", func(t *testing.T) {
		parallelism := abstract.NewFakeShardingTaskRuntime(0, 2, 777, 999)
		_, err := NewEffectiveWorkerNum(logger.Log, parallelism, true)
		require.Error(t, err)
	})

	t.Run("", func(t *testing.T) {
		parallelism := abstract.NewFakeShardingTaskRuntime(1, 2, 777, 999)
		effectiveWorkerNum, err := NewEffectiveWorkerNum(logger.Log, parallelism, true)
		require.NoError(t, err)
		require.Equal(t, 0, effectiveWorkerNum.CurrentWorkerNum)
		require.Equal(t, 1, effectiveWorkerNum.WorkersCount)
	})
}
