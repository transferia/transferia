package parsequeue

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
)

type mockSink struct {
	push func(items []abstract.ChangeItem) chan error
}

func (m mockSink) Close() error {
	return nil
}

func (m mockSink) AsyncPush(items []abstract.ChangeItem) chan error {
	return m.push(items)
}

func TestSinkNotBlocking(t *testing.T) {
	parallelism := 10
	pushCnt := atomic.Int32{}
	ackCnt := atomic.Int32{}
	parseCnt := atomic.Int32{}
	q := New[int](logger.Log, parallelism, &mockSink{
		push: func(items []abstract.ChangeItem) chan error {
			pushCnt.Add(1)
			resCh := make(chan error) // push never finished
			return resCh
		},
	}, func(data int) []abstract.ChangeItem {
		parseCnt.Add(1)
		return nil // immediate parse
	}, func(data int, _ time.Time, _ error) {
		ackCnt.Add(1)
	})
	for i := 0; i < 1000; i++ {
		require.NoError(t, q.Add(i))
	}
	st := time.Now()
	for time.Since(st) < time.Second {
		if pushCnt.Load() == int32(1000) {
			break
		} else {
			time.Sleep(time.Millisecond)
		}
	}
	require.Equal(t, parseCnt.Load(), int32(1000))
	require.Equal(t, pushCnt.Load(), int32(1000))
	require.Equal(t, ackCnt.Load(), int32(0))
}

func TestAckOrder(t *testing.T) {
	wgMap := map[int]*sync.WaitGroup{}
	parallelism := 10
	for i := 0; i < parallelism*10; i++ {
		wgMap[i] = &sync.WaitGroup{}
		wgMap[i].Add(1)
	}
	var res []int
	mu := sync.Mutex{}
	inflight := atomic.Int32{}
	q := New[int](logger.Log, parallelism, &mockSink{
		push: func(items []abstract.ChangeItem) chan error {
			resCh := make(chan error, 1)
			resCh <- nil
			return resCh
		},
	}, func(data int) []abstract.ChangeItem {
		inflight.Add(1)
		defer inflight.Add(-1)
		wgMap[data].Wait()
		return nil
	}, func(data int, _ time.Time, _ error) {
		mu.Lock()
		defer mu.Unlock()
		res = append(res, data)
	})
	// all 10 parse func must be called
	go func() {
		for i := 0; i < parallelism*10; i++ {
			_ = q.Add(i) // add is blocking call, so we can't write more parallelism in a same time
		}
	}()
	time.Sleep(time.Second)
	mu.Lock()
	require.Equal(t, int32(parallelism), inflight.Load())
	mu.Unlock()
	// but result still pending, we wait for wait groups
	require.Len(t, res, 0)
	// done wait group in reverse order
	for _, wg := range wgMap {
		wg.Done()
	}
	mu.Lock()
	for i, data := range res {
		require.Equal(t, i, data) // order should be same
	}
	mu.Unlock()
	q.Close()
}

func TestGracefullyShutdown(t *testing.T) {
	var res []int
	mu := sync.Mutex{}
	q := New[int](logger.Log, 5, &mockSink{
		push: func(items []abstract.ChangeItem) chan error {
			resCh := make(chan error, 1)
			resCh <- nil
			time.Sleep(2 * time.Millisecond)
			return resCh
		},
	}, func(data int) []abstract.ChangeItem {
		time.Sleep(time.Millisecond)
		return nil
	}, func(data int, _ time.Time, _ error) {
		mu.Lock()
		defer mu.Unlock()
		res = append(res, data)
	})
	go func() {
		iter := 0
		for {
			if err := q.Add(iter); err != nil {
				return
			}
			iter++
			time.Sleep(10 * time.Millisecond)
		}
	}()
	time.Sleep(time.Second)
	q.Close()
}

func TestRandomParseDelay(t *testing.T) {
	parallelism := 10
	inputEventsCount := 100

	mu := sync.Mutex{}
	counter := 0
	validateCounter := func(counter int) {
		require.LessOrEqual(t, counter, parallelism)
	}
	ackIter := 0
	pushIter := 0
	q := New[int](logger.Log, parallelism, &mockSink{
		push: func(items []abstract.ChangeItem) chan error {
			resCh := make(chan error, 1)
			resCh <- nil
			require.Equal(t, pushIter, items[0].ColumnValues[0].(int))
			mu.Lock()
			defer mu.Unlock()
			pushIter++
			return resCh
		},
	}, func(data int) []abstract.ChangeItem {
		mu.Lock()
		fmt.Printf("%d STARTED, counter:%d->%d\n", data, counter, counter+1)
		counter++
		validateCounter(counter)
		mu.Unlock()

		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

		mu.Lock()
		fmt.Printf("%d FINISHED, counter:%d->%d\n", data, counter, counter-1)
		validateCounter(counter)
		counter--
		mu.Unlock()
		return []abstract.ChangeItem{{ColumnValues: []any{data}}}
	}, func(data int, _ time.Time, _ error) {
		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, ackIter, data)
		ackIter++
	})
	for i := 0; i < inputEventsCount; i++ {
		require.NoError(t, q.Add(i))
	}
	for {
		mu.Lock()
		if ackIter == inputEventsCount && pushIter == inputEventsCount {
			mu.Unlock()
			break
		} else {
			time.Sleep(time.Millisecond)
		}
		mu.Unlock()
	}
	q.Close()
}
