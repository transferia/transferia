package parsequeue

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	abstract "github.com/transferia/transferia/pkg/abstract"
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
	q := NewWaitable[int](logger.Log, parallelism, &mockSink{
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
		fmt.Printf("%d ACKED\n", ackIter)
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
