package buffer

import (
	"bytes"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func TestBufferPool(t *testing.T) {
	pool := NewBufferPool(10)

	capBefore := 0

	for i := 0; i < 10; i++ {
		buf := pool.Get(t.Context())
		buf.Write([]byte("test"))
		if i == 0 {
			capBefore = buf.Cap()
		}

		require.Equal(t, capBefore, buf.Cap())
		pool.Put(buf)
	}

	capAfter := 0
	for i := 0; i < 10; i++ {
		buf := pool.Get(t.Context())
		if i == 0 {
			capAfter = buf.Cap()
			require.Equal(t, capBefore, capAfter)
		}

		require.Equal(t, capAfter, buf.Cap())
		pool.Put(buf)
	}
}

// we should not panic if size is 0 and create a pool with size 1 to avoid deadlock
func TestBufferPool_ZeroSize(t *testing.T) {
	pool := NewBufferPool(0)
	require.NotNil(t, pool)
	require.Equal(t, 1, pool.size)
}

func TestBufferPoolPutWhenFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := NewBufferPool(1)
		pooled := pool.Get(t.Context())
		pool.Put(pooled)
		extra := bytes.NewBufferString("extra")

		done := make(chan struct{})
		go func() {
			pool.Put(extra)
			close(done)
		}()
		synctest.Wait()

		select {
		case <-done:
		default:
			pool.Get(t.Context()) // Release the blocked sender before ending the test.
			t.Fatal("Put blocked on a full pool")
		}
		require.Same(t, pooled, pool.Get(t.Context()))
		require.Equal(t, "extra", extra.String())
	})
}
