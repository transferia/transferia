package parsequeue

import (
	"context"
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
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
)

func TestSinkNotBlocking(t *testing.T) {
	parallelism := 10
	pushCnt := atomic.Int32{}
	ackCnt := atomic.Int32{}
	parseCnt := atomic.Int32{}
	q := New[int](logger.Log, parallelism, mocksink.NewMockAsyncSinkWithChan(func(items []abstract.ChangeItem) chan error {
		pushCnt.Add(1)
		resCh := make(chan error) // push never finished
		return resCh
	}), func(data int) ([]abstract.ChangeItem, error) {
		parseCnt.Add(1)
		return nil, nil // immediate parse
	}, func(data int, _ time.Time) error {
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
	require.NoError(t, q.Error())

	q.Close()
	require.NoError(t, q.Error())
}

func TestParseErrNotBlocking(t *testing.T) {
	parallelism := 10
	pushCnt := atomic.Int32{}
	ackCnt := atomic.Int32{}
	parseCnt := atomic.Int32{}
	testError := xerrors.New("test error")
	q := New(logger.Log, parallelism, mocksink.NewMockAsyncSinkWithChan(func(items []abstract.ChangeItem) chan error {
		pushCnt.Add(1)
		return nil
	}), func(data int) ([]abstract.ChangeItem, error) {
		parseCnt.Add(1)
		return nil, testError
	}, func(data int, _ time.Time) error {
		ackCnt.Add(1)
		return nil
	})
	for i := range 1000 {
		_ = q.Add(i)
	}
	st := time.Now()
	for time.Since(st) < time.Second {
		if ackCnt.Load() == int32(1000) {
			break
		} else {
			time.Sleep(time.Millisecond)
		}
	}
	require.LessOrEqual(t, parseCnt.Load(), int32(1000))
	require.Equal(t, pushCnt.Load(), int32(0))
	require.Equal(t, ackCnt.Load(), int32(0))
	require.Error(t, q.Error())

	q.Close()
	require.Error(t, q.Error())
	require.ErrorIs(t, q.Error(), testError)
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
	q := New[int](logger.Log, parallelism, mocksink.NewMockAsyncSinkWithChan(func(items []abstract.ChangeItem) chan error {
		return nil
	}), func(data int) ([]abstract.ChangeItem, error) {
		inflight.Add(1)
		defer inflight.Add(-1)
		wgMap[data].Wait()
		return nil, nil
	}, func(data int, _ time.Time) error {
		mu.Lock()
		defer mu.Unlock()
		res = append(res, data)

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
	require.NoError(t, q.Error())
}

func TestGracefullyShutdown(t *testing.T) {
	var res []int
	mu := sync.Mutex{}
	q := New[int](logger.Log, 5, mocksink.NewMockAsyncSinkWithChan(func(items []abstract.ChangeItem) chan error {
		time.Sleep(2 * time.Millisecond)
		return nil
	}), func(data int) ([]abstract.ChangeItem, error) {
		time.Sleep(time.Millisecond)
		return nil, nil
	}, func(data int, _ time.Time) error {
		mu.Lock()
		defer mu.Unlock()
		res = append(res, data)
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
	time.Sleep(time.Second)
	q.Close()
	require.NoError(t, q.Error())
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
	q := New[int](logger.Log, parallelism, mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
		require.Equal(t, pushIter, items[0].ColumnValues[0].(int))
		mu.Lock()
		defer mu.Unlock()
		pushIter++
		return nil
	}), func(data int) ([]abstract.ChangeItem, error) {
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
	}, func(data int, _ time.Time) error {
		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, ackIter, data)
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
	require.NoError(t, q.Error())
}

func TestErrorPropagation(t *testing.T) {
	t.Run("ParseError", func(t *testing.T) {
		parseErrMsg := "parse failed on message 5"
		parseCnt := atomic.Int32{}
		ackCnt := atomic.Int32{}

		q := New[int](logger.Log, 5, mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
			return nil
		}), func(data int) ([]abstract.ChangeItem, error) {
			parseCnt.Add(1)
			if data == 5 {
				return nil, xerrors.New(parseErrMsg)
			}
			return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
		}, func(data int, _ time.Time) error {
			ackCnt.Add(1)
			return nil
		})

		for i := 0; i < 10; i++ {
			if err := q.Add(i); err != nil {
				break
			}
		}

		time.Sleep(300 * time.Millisecond)
		q.Close()

		require.Error(t, q.Error())
		require.Contains(t, q.Error().Error(), "parsing error")
		require.Contains(t, q.Error().Error(), parseErrMsg)

		err := q.Add(999)
		require.Error(t, err)
		require.Contains(t, err.Error(), "parse queue is already closed")

		require.LessOrEqual(t, ackCnt.Load(), int32(5))
	})

	t.Run("PushError", func(t *testing.T) {
		pushErrMsg := "async push failed"
		pushCnt := atomic.Int32{}
		ackCnt := atomic.Int32{}

		q := New[int](logger.Log, 5, mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
			pushCnt.Add(1)
			if pushCnt.Load() == 3 {
				return xerrors.New(pushErrMsg)
			}
			return nil
		}), func(data int) ([]abstract.ChangeItem, error) {
			return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
		}, func(data int, _ time.Time) error {
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

		q := New[int](logger.Log, 5, mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
			return nil
		}), func(data int) ([]abstract.ChangeItem, error) {
			return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
		}, func(data int, _ time.Time) error {
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

		q := New[int](logger.Log, 10, mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
			return nil
		}), func(data int) ([]abstract.ChangeItem, error) {
			return nil, nil
		}, func(data int, _ time.Time) error {
			return xerrors.Errorf("ack error %d", data)
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
	t.Run("AddAfterClose", func(t *testing.T) {
		q := New[int](logger.Log, 5, mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
			return nil
		}), func(data int) ([]abstract.ChangeItem, error) {
			return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
		}, func(data int, _ time.Time) error {
			return nil
		})

		q.Close()
		require.NoError(t, q.Error())

		err := q.Add(100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "parse queue is already closed")
	})

	t.Run("MultipleCloseCalls", func(t *testing.T) {
		q := New[int](logger.Log, 5, mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
			return nil
		}), func(data int) ([]abstract.ChangeItem, error) {
			return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
		}, func(data int, _ time.Time) error {
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

	t.Run("CloseIdempotency", func(t *testing.T) {
		q := New[int](logger.Log, 5, mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
			return nil
		}), func(data int) ([]abstract.ChangeItem, error) {
			return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
		}, func(data int, _ time.Time) error {
			return nil
		})

		q.Close()
		q.Close()
		q.Close()

		require.NoError(t, q.Error())
		require.ErrorIs(t, q.ctx.Err(), context.Canceled)
	})
}
