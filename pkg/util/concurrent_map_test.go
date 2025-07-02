package util

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConcurrentMapSetGetDelete(t *testing.T) {
	m := NewConcurrentMap[string, int]()

	m.Set("a", 1)
	m.Set("b", 2)

	keys := m.ListKeys()
	require.Equal(t, 2, len(keys))

	v, ok := m.Get("a")
	require.True(t, ok)
	require.Equal(t, 1, v)

	v, ok = m.Get("b")
	require.True(t, ok)
	require.Equal(t, 2, v)

	_, ok = m.Get("c")
	require.False(t, ok)

	v, ok = m.Delete("a")
	require.True(t, ok)
	require.Equal(t, 1, v)

	_, ok = m.Get("a")
	require.False(t, ok)

	_, ok = m.Delete("a")
	require.False(t, ok)
}

func TestConcurrentMapConcurrentAccess(t *testing.T) {
	m := NewConcurrentMap[int, int]()
	wg := sync.WaitGroup{}

	for i := range 1000 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Set(i, i*i)
		}(i)
	}

	for i := range 1000 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = m.Get(i)
		}(i)
	}

	wg.Wait()

	require.Equal(t, 1000, m.Len())

	for i := range 1000 {
		v, ok := m.Get(i)
		require.True(t, ok)
		require.Equal(t, i*i, v)
	}
}

func TestConcurrentMapClear(t *testing.T) {
	m := NewConcurrentMap[int, int]()

	for i := range 1000 {
		m.Set(i, i*i)
	}

	require.Equal(t, 1000, m.Len())

	m.Clear(func(mp map[int]int) {
		for k := range mp {
			require.Equal(t, k*k, mp[k])
		}
	})

	require.Equal(t, 0, m.Len())
}
