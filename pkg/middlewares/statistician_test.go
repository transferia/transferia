package middlewares

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/stats"
)

type statisticianTestSink struct {
	push func([]abstract.ChangeItem) error
}

func (s *statisticianTestSink) Push(items []abstract.ChangeItem) error {
	return s.push(items)
}

func (s *statisticianTestSink) Close() error {
	return nil
}

var _ abstract.Sinker = (*statisticianTestSink)(nil)

func TestStatisticianLagMeasurements(t *testing.T) {
	registry := solomon.NewRegistry(nil)
	values := func() map[string]float64 {
		metrics, err := registry.Gather()
		require.NoError(t, err)
		result := make(map[string]float64)
		for _, metric := range metrics.List() {
			if metric.Name() == "sinker.pusher.time.row_max_lag_sec" || metric.Name() == "sinker.pusher.time.row_max_read_lag_sec" {
				result[metric.Name()] = metric.Value().(float64)
			}
		}
		return result
	}
	pushError := xerrors.New("destination unavailable")
	sink := &statisticianTestSink{push: func(_ []abstract.ChangeItem) error {
		// Read lag must be published before attempting the destination write.
		require.GreaterOrEqual(t, values()["sinker.pusher.time.row_max_read_lag_sec"], 60.0)
		require.NotContains(t, values(), "sinker.pusher.time.row_max_lag_sec")
		return pushError
	}}
	stat := newStatistician(sink, logger.Log, stats.NewWrapperStats(registry))
	t.Cleanup(func() { require.NoError(t, stat.Close()) })
	require.Empty(t, values())
	items := []abstract.ChangeItem{{Kind: abstract.InsertKind, CommitTime: uint64(time.Now().Add(-time.Minute).UnixNano())}}
	require.ErrorIs(t, stat.Push(items), pushError)
	require.NotContains(t, values(), "sinker.pusher.time.row_max_lag_sec")
	sink.push = func(_ []abstract.ChangeItem) error { return nil }
	require.NoError(t, stat.Push(items))
	require.GreaterOrEqual(t, values()["sinker.pusher.time.row_max_lag_sec"], 60.0)
}
