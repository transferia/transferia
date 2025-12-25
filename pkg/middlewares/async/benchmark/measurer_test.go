package async

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/middlewares/async"
	"github.com/transferia/transferia/pkg/middlewares/async/bufferer"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/tests/canon/reference"
)

type mockAsyncSink struct {
	mu         sync.Mutex
	totalBytes uint64
}

func (m *mockAsyncSink) Close() error { return nil }

func (m *mockAsyncSink) AsyncPush(items []abstract.ChangeItem) chan error {
	res := make(chan error, 1)
	go func(items []abstract.ChangeItem, resCh chan error) {
		m.mu.Lock()
		for _, row := range items {
			m.totalBytes += row.Size.Values
		}
		m.mu.Unlock()
		res <- nil
	}(items, res)
	return res
}

type mockSink struct{}

func (m *mockSink) Push(items []abstract.ChangeItem) error { return nil }

func (m *mockSink) Close() error { return nil }

func TestMeasurer(t *testing.T) {
	schema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "a", PrimaryKey: true, DataType: "integer"},
		{ColumnName: "b", PrimaryKey: false, DataType: "integer"},
		{ColumnName: "c", PrimaryKey: false, DataType: "string"},
	})

	items := []abstract.ChangeItem{
		{
			Kind:         "insert",
			ColumnNames:  []string{"a", "b", "c"},
			ColumnValues: []interface{}{1, 2, "123"},
			TableSchema:  schema,
		},
		{
			Kind:         "insert",
			ColumnNames:  []string{"a", "b", "c"},
			ColumnValues: []interface{}{2, 3, "456"},
			TableSchema:  schema,
		},
	}

	mock := &mockAsyncSink{}
	sinker := async.Measurer(logger.Log)(mock)

	pushResult := sinker.AsyncPush(items)
	pushError := <-pushResult
	require.NoError(t, pushError)
	require.Equal(t, mock.totalBytes, uint64(214))
}

func BenchmarkMeasurer(b *testing.B) {
	b.Run("benchmark pipeline with measure", func(b *testing.B) {
		for _, count := range []int{1, 1000, 100_000} {
			var input []abstract.ChangeItem
			for i := 0; i < count; i++ {
				input = append(input, reference.Table()[0])
			}
			b.Run(fmt.Sprintf("row_%d", count), func(b *testing.B) {
				target := &mockSink{}
				asyncSinker := makeAsyncSink(target, true)
				for i := 0; i < b.N; i++ {
					b.StartTimer()
					resCh := asyncSinker.AsyncPush(input)
					require.NoError(b, <-resCh)
					b.StopTimer()
				}
				b.ReportAllocs()
			})
		}
	})

	b.Run("benchmark pipeline without measure", func(b *testing.B) {
		for _, count := range []int{1, 1000, 100_000} {
			var input []abstract.ChangeItem
			for i := 0; i < count; i++ {
				input = append(input, reference.Table()[0])
			}
			b.Run(fmt.Sprintf("row_%d", count), func(b *testing.B) {
				target := &mockSink{}
				asyncSinker := makeAsyncSink(target, false)
				for i := 0; i < b.N; i++ {
					b.StartTimer()
					resCh := asyncSinker.AsyncPush(input)
					require.NoError(b, <-resCh)
					b.StopTimer()
				}
				b.ReportAllocs()
			})
		}
	})
}

func makeAsyncSink(sinker abstract.Sinker, addMeasurer bool) abstract.AsyncSink {
	mtrcs := solomon.NewRegistry(solomon.NewRegistryOpts())
	pipeline := middlewares.Statistician(logger.Log, stats.NewWrapperStats(mtrcs))(sinker)
	pipeline = middlewares.NonRowSeparator()(pipeline)
	asyncSinker := bufferer.Bufferer(logger.Log, bufferer.BuffererConfig{TriggingCount: 1000, TriggingSize: 0, TriggingInterval: time.Second}, mtrcs)(pipeline)
	if addMeasurer {
		asyncSinker = async.Measurer(logger.Log)(asyncSinker)
	}
	return asyncSinker
}
