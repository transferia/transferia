package parsequeue

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
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
)

func TestRandomParseDelayWithEnsure(t *testing.T) {
	parallelism := 10
	inputEventsCount := 100

	mu := sync.Mutex{}
	counter := 0
	validateCounter := func(counter int) {
		require.LessOrEqual(t, counter, parallelism)
	}
	ackIter := 0
	pushIter := 0
	q := NewWaitable[int](logger.Log, parallelism, mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
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
		fmt.Printf("%d ACKED\n", ackIter)
		return nil
	})
	for i := 0; i < inputEventsCount; i++ {
		require.NoError(t, q.Add(i))
	}
	q.Wait()
	require.Equal(t, ackIter, inputEventsCount)
	pushIter = 0
	ackIter = 0
	for i := 0; i < inputEventsCount; i++ {
		require.NoError(t, q.Add(i))
	}
	q.Wait()
	require.Equal(t, ackIter, inputEventsCount)
	q.Close()
	require.NoError(t, q.Error())
}

func TestWaitWithError(t *testing.T) {
	ackCnt := atomic.Int32{}

	testError := xerrors.New("ack error at 5")
	q := NewWaitable[int](logger.Log, 5, mocksink.NewMockAsyncSink(func(items []abstract.ChangeItem) error {
		return nil
	}), func(data int) ([]abstract.ChangeItem, error) {
		return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
	}, func(data int, _ time.Time) error {
		if ackCnt.Add(1) == 5 {
			return testError
		}
		return nil
	})

	for i := 0; i < 10; i++ {
		_ = q.Add(i)
	}
	q.Wait()

	q.Close()
	require.Error(t, q.parseQueue.Error())
	require.ErrorIs(t, q.parseQueue.Error(), testError)
}
