package ordered_multimap

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/dispatcher/file"
)

func TestFindClosestKey(t *testing.T) {
	nsecToTime := func(nsec int64) time.Time {
		return time.Unix(0, nsec)
	}

	gen := func(ns int) (int64, *file.File) {
		return int64(ns), &file.File{
			FileName:     fmt.Sprintf("%d", ns),
			FileSize:     int64(ns),
			LastModified: nsecToTime(int64(ns)),
		}
	}

	t.Run("equal", func(t *testing.T) {
		currMap := NewOrderedMultiMap()
		_ = currMap.Add(gen(1))
		key, err := currMap.FindClosestKey(1)
		require.NoError(t, err)
		require.Equal(t, int64(1), key)
	})

	t.Run("before", func(t *testing.T) {
		currMap := NewOrderedMultiMap()
		_ = currMap.Add(gen(1))
		key, err := currMap.FindClosestKey(0)
		require.NoError(t, err)
		require.Equal(t, int64(0), key)
	})

	t.Run("between", func(t *testing.T) {
		currMap := NewOrderedMultiMap()
		_ = currMap.Add(gen(1))
		_ = currMap.Add(gen(3))
		key, err := currMap.FindClosestKey(2)
		require.NoError(t, err)
		require.Equal(t, int64(1), key)
	})
}
