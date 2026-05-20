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
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
)

func TestRandomParseDelayWithEnsureQueueToS3(t *testing.T) {
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

	q := NewWaitable[int](logger.Log, parallelism, mockSinkToS3, func(data int) ([]abstract.ChangeItem, error) {
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
}

func TestWaitWithError(t *testing.T) {
	ackCnt := atomic.Int32{}

	mockSinkToS3 := mocksink.NewMockQueueToS3Sink(func(item abstract.ChangeItem) uint64 {
		return uint64(item.ColumnValues[0].(int))
	}, func(items []abstract.ChangeItem) error {
		return nil
	}, nil)

	q := NewWaitable[int](logger.Log, 5, mockSinkToS3, func(data int) ([]abstract.ChangeItem, error) {
		return []abstract.ChangeItem{{ColumnValues: []any{data}}}, nil
	}, func(pushResult abstract.QueueResult) error {
		if ackCnt.Add(1) == 5 {
			return xerrors.New("ack error at 5")
		}
		return nil
	})

	for i := 0; i < 10; i++ {
		_ = q.Add(i)
	}
	q.Wait()

	q.Close()

	require.Error(t, q.parseQueue.Error())
	require.Contains(t, q.parseQueue.Error().Error(), "ack error")
}
