package queue_to_s3_parsequeue

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsequeue"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
)

func TestSinkNotBlockingQueueToS3(t *testing.T) {
	parallelism := 10
	pushCnt := atomic.Int32{}
	ackCnt := atomic.Int32{}
	parseCnt := atomic.Int32{}

	mockSinkToS3 := mocksink.NewMockQueueToS3Sink(func(item abstract.ChangeItem) uint64 {
		return 0
	}, func(items []abstract.ChangeItem) error {
		pushCnt.Add(1)
		return nil
	}, func(_ []uint64) bool {
		return false // push never finished
	})

	q := New[int](logger.Log, parallelism, mockSinkToS3, func(data int) ([]abstract.ChangeItem, error) {
		parseCnt.Add(1)
		return nil, nil // immediate parse
	}, func(res abstract.QueueResult) error {
		ackCnt.Add(1)
		return nil
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

func TestParseErrNotBlockingQueueToS3(t *testing.T) {
	parallelism := 10
	pushCnt := atomic.Int32{}
	ackCnt := atomic.Int32{}
	parseCnt := atomic.Int32{}

	mockSinkToS3 := mocksink.NewMockQueueToS3Sink(func(item abstract.ChangeItem) uint64 {
		return 0
	}, func(items []abstract.ChangeItem) error {
		pushCnt.Add(1)
		return nil
	}, nil)

	q := New(logger.Log, parallelism, mockSinkToS3, func(data int) ([]abstract.ChangeItem, error) {
		parseCnt.Add(1)
		if data == 0 {
			return nil, xerrors.New("test error")
		}
		return nil, nil
	}, func(res abstract.QueueResult) error {
		ackCnt.Add(1)
		return nil
	})

	i := 0
	for {
		if err := q.Add(i); err != nil {
			if err == parsequeue.ErrorWhileParsing { // Received error after parsing already started
				_ = parseCnt.Swap(int32(i))
			}
			break
		}
		i++
	}

	require.Equal(t, parseCnt.Load(), int32(i))
	require.Equal(t, pushCnt.Load(), int32(0))
	require.Equal(t, ackCnt.Load(), int32(0))
}

func TestAckOrderQueueToS3(t *testing.T) {
	wgMap := map[int]*sync.WaitGroup{}
	parallelism := 10
	for i := 0; i < parallelism*10; i++ {
		wgMap[i] = &sync.WaitGroup{}
		wgMap[i].Add(1)
	}
	var resOffsets []uint64
	mu := sync.Mutex{}
	inflight := atomic.Int32{}

	mockSinkToS3 := mocksink.NewMockQueueToS3Sink(func(item abstract.ChangeItem) uint64 {
		return uint64(item.ColumnValues[0].(int))
	}, func(items []abstract.ChangeItem) error {
		return nil
	}, nil)

	q := New[int](logger.Log, parallelism, mockSinkToS3, func(data int) ([]abstract.ChangeItem, error) {
		inflight.Add(1)
		defer inflight.Add(-1)
		wgMap[data].Wait()
		return []abstract.ChangeItem{{ColumnValues: []any{uint64(data)}}}, nil
	}, func(pushResult abstract.QueueResult) error {
		mu.Lock()
		defer mu.Unlock()
		resOffsets = append(resOffsets, pushResult.Offsets...)
		return nil
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
	require.Len(t, resOffsets, 0)
	// done wait group in reverse order
	for _, wg := range wgMap {
		wg.Done()
	}
	mu.Lock()
	for i, data := range resOffsets {
		require.Equal(t, i, data) // order should be same
	}
	mu.Unlock()
	q.Close()
}

func TestGracefullyShutdownQueueToS3(t *testing.T) {
	mu := sync.Mutex{}

	mockSinkToS3 := mocksink.NewMockQueueToS3Sink(func(item abstract.ChangeItem) uint64 {
		return 0
	}, func(items []abstract.ChangeItem) error {
		return nil
	}, nil)

	q := New[int](logger.Log, 5, mockSinkToS3, func(data int) ([]abstract.ChangeItem, error) {
		time.Sleep(time.Millisecond)
		return nil, nil
	}, func(pushResult abstract.QueueResult) error {
		mu.Lock()
		defer mu.Unlock()
		return nil
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

	time.Sleep(500 * time.Millisecond)
	q.Close()

	require.NoError(t, q.firstErr)
}

func TestRandomParseDelayQueueToS3(t *testing.T) {
	parallelism := 10
	inputEventsCount := 100

	mu := sync.Mutex{}
	counter := 0
	validateCounter := func(counter int) {
		require.LessOrEqual(t, counter, parallelism)
	}
	ackIter := 0
	pushIter := 0

	mockSinkToS3 := mocksink.NewMockQueueToS3Sink(func(item abstract.ChangeItem) uint64 {
		return uint64(item.ColumnValues[0].(int))
	}, func(items []abstract.ChangeItem) error {
		require.Equal(t, pushIter, items[0].ColumnValues[0].(int))
		mu.Lock()
		defer mu.Unlock()
		pushIter++
		return nil
	}, nil)

	q := New[int](logger.Log, parallelism, mockSinkToS3, func(data int) ([]abstract.ChangeItem, error) {
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
		return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
	}, func(pushResult abstract.QueueResult) error {
		mu.Lock()
		defer mu.Unlock()
		require.Len(t, pushResult.Offsets, 1)
		require.Equal(t, ackIter, int(pushResult.Offsets[0]))
		ackIter++
		return nil
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

func TestErrorPropagation(t *testing.T) {
	t.Run("PushError", func(t *testing.T) {
		pushErrMsg := "async push failed"
		pushCnt := atomic.Int32{}
		ackCnt := atomic.Int32{}

		mockSinkToS3 := mocksink.NewMockQueueToS3Sink(func(item abstract.ChangeItem) uint64 {
			return 0
		}, func(items []abstract.ChangeItem) error {
			pushCnt.Add(1)
			if pushCnt.Load() == 3 {
				return xerrors.New(pushErrMsg)
			}
			return nil
		}, nil)

		q := New[int](logger.Log, 5, mockSinkToS3, func(data int) ([]abstract.ChangeItem, error) {
			return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
		}, func(pushResult abstract.QueueResult) error {
			ackCnt.Add(1)
			return nil
		})

		for i := 0; i < 10; i++ {
			err := q.Add(i)
			if err != nil {
				break
			}
		}

		time.Sleep(300 * time.Millisecond)
		q.Close()

		require.Error(t, q.Error())
		require.Contains(t, q.Error().Error(), "push error")
		require.Contains(t, q.Error().Error(), pushErrMsg)

		require.LessOrEqual(t, ackCnt.Load(), int32(2))
	})

	t.Run("AckError", func(t *testing.T) {
		ackErrMsg := "ack callback failed"
		ackCnt := atomic.Int32{}

		mockSinkToS3 := mocksink.NewMockQueueToS3Sink(func(item abstract.ChangeItem) uint64 {
			return 0
		}, func(items []abstract.ChangeItem) error {
			return nil
		}, nil)

		q := New[int](logger.Log, 5, mockSinkToS3, func(data int) ([]abstract.ChangeItem, error) {
			return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
		}, func(pushResult abstract.QueueResult) error {
			ackCnt.Add(1)
			if ackCnt.Load() == 3 {
				return xerrors.New(ackErrMsg)
			}
			return nil
		})

		for i := 0; i < 10; i++ {
			err := q.Add(i)
			if err != nil {
				break
			}
		}

		time.Sleep(200 * time.Millisecond)
		q.Close()

		require.Error(t, q.Error())
		require.Contains(t, q.Error().Error(), "ack error")
		require.Contains(t, q.Error().Error(), ackErrMsg)

		require.Equal(t, int32(3), ackCnt.Load())
	})

	t.Run("MultipleErrorsAndFirstWins", func(t *testing.T) {
		mockSinkToS3 := mocksink.NewMockQueueToS3Sink(func(item abstract.ChangeItem) uint64 {
			return uint64(item.ColumnValues[0].(int))
		}, func(items []abstract.ChangeItem) error {
			return nil
		}, nil)

		q := New[int](logger.Log, 10, mockSinkToS3, func(data int) ([]abstract.ChangeItem, error) {
			return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
		}, func(pushResult abstract.QueueResult) error {
			return xerrors.Errorf("ack error %d", pushResult.Offsets[0])
		})

		for i := 0; i < 20; i++ {
			if err := q.Add(i); err != nil {
				break
			}
		}

		time.Sleep(200 * time.Millisecond)
		q.Close()

		require.Error(t, q.Error())
		require.Contains(t, q.Error().Error(), "ack error 0")
	})
}

func TestClose(t *testing.T) {
	mockSinkToS3 := mocksink.NewMockQueueToS3Sink(func(item abstract.ChangeItem) uint64 {
		return 0
	}, func(items []abstract.ChangeItem) error {
		return nil
	}, nil)

	t.Run("AddAfterClose", func(t *testing.T) {
		q := New[int](logger.Log, 5, mockSinkToS3, func(data int) ([]abstract.ChangeItem, error) {
			return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
		}, func(pushResult abstract.QueueResult) error {
			return nil
		})

		q.Close()
		require.NoError(t, q.Error())

		err := q.Add(100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "parse queue is already closed")
	})

	t.Run("MultipleCloseCalls", func(t *testing.T) {
		q := New[int](logger.Log, 5, mockSinkToS3, func(data int) ([]abstract.ChangeItem, error) {
			return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
		}, func(pushResult abstract.QueueResult) error {
			return nil
		})

		for i := 0; i < 20; i++ {
			require.NoError(t, q.Add(i))
		}

		wg := sync.WaitGroup{}
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				q.Close()
			}()
		}
		wg.Wait()

		require.NoError(t, q.Error())
	})
}
