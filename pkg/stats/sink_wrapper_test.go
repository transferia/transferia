package stats

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
)

func TestResetMaxLag(t *testing.T) {
	resetMaxLagForTest()

	registry := solomon.NewRegistry(solomon.NewRegistryOpts())
	stats := NewWrapperStats(registry)

	// Store some lag value
	stats.storeMaxLag(5 * time.Second)
	require.Greater(t, maxLagValue.Load(), uint64(0), "MaxLag should be set")

	// Reset should clear the value
	stats.ResetMaxLag()
	require.Equal(t, uint64(0), maxLagValue.Load(), "MaxLag should be reset to 0")
}

func TestStoreMaxLag(t *testing.T) {
	resetMaxLagForTest()

	registry := solomon.NewRegistry(solomon.NewRegistryOpts())
	stats := NewWrapperStats(registry)

	t.Run("store first lag value", func(t *testing.T) {
		maxLagValue.Store(0)
		stats.storeMaxLag(3 * time.Second)
		stored := maxLagValue.Load()
		require.Equal(t, uint64((3 * time.Second).Nanoseconds()), stored)
	})

	t.Run("store higher lag value", func(t *testing.T) {
		maxLagValue.Store(uint64((3 * time.Second).Nanoseconds()))
		stats.storeMaxLag(5 * time.Second)
		stored := maxLagValue.Load()
		require.Equal(t, uint64((5 * time.Second).Nanoseconds()), stored)
	})

	t.Run("do not store lower lag value", func(t *testing.T) {
		maxLagValue.Store(uint64((5 * time.Second).Nanoseconds()))
		stats.storeMaxLag(2 * time.Second)
		stored := maxLagValue.Load()
		require.Equal(t, uint64((5 * time.Second).Nanoseconds()), stored, "should keep higher value")
	})

	t.Run("do not store zero lag", func(t *testing.T) {
		maxLagValue.Store(uint64((5 * time.Second).Nanoseconds()))
		stats.storeMaxLag(0)
		stored := maxLagValue.Load()
		require.Equal(t, uint64((5 * time.Second).Nanoseconds()), stored, "should not store zero")
	})
}

func TestStoreMaxLagConcurrent(t *testing.T) {
	resetMaxLagForTest()

	registry := solomon.NewRegistry(solomon.NewRegistryOpts())
	stats := NewWrapperStats(registry)

	// Test concurrent updates to maxLag
	var wg sync.WaitGroup
	goroutines := 100
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(val int) {
			defer wg.Done()
			stats.storeMaxLag(time.Duration(val) * time.Millisecond)
		}(i)
	}

	wg.Wait()

	// The final value should be the maximum (99 milliseconds)
	stored := maxLagValue.Load()
	expected := uint64((99 * time.Millisecond).Nanoseconds())
	require.Equal(t, expected, stored, "should store the maximum value from concurrent updates")
}

func TestMultipleSinksSharedMaxLag(t *testing.T) {
	resetMaxLagForTest()

	// Create multiple sinks with the same registry
	registry := solomon.NewRegistry(solomon.NewRegistryOpts())
	stats1 := NewWrapperStats(registry)
	stats2 := NewWrapperStats(registry)
	require.Equal(t, stats1.MaxLag, stats2.MaxLag)

	// Both should share the same maxLagValue
	stats1.storeMaxLag(3 * time.Second)
	require.Equal(t, uint64((3 * time.Second).Nanoseconds()), maxLagValue.Load())

	stats2.storeMaxLag(5 * time.Second)
	require.Equal(t, uint64((5 * time.Second).Nanoseconds()), maxLagValue.Load())

	// Reset from one should affect both
	stats1.ResetMaxLag()
	require.Equal(t, uint64(0), maxLagValue.Load())
}

func TestGetOrCreateMaxLagGauge(t *testing.T) {
	resetMaxLagForTest()

	t.Run("creates gauge for new registry", func(t *testing.T) {
		registry := solomon.NewRegistry(solomon.NewRegistryOpts())
		gauge := getOrCreateMaxLagGauge(registry)
		require.NotNil(t, gauge)
		require.Len(t, maxLagGaugeRegistry, 1)
	})

	t.Run("reuses gauge for same registry", func(t *testing.T) {
		registry := solomon.NewRegistry(solomon.NewRegistryOpts())
		gauge1 := getOrCreateMaxLagGauge(registry)
		gauge2 := getOrCreateMaxLagGauge(registry)
		require.Equal(t, gauge1, gauge2, "should return the same gauge instance")
	})

	t.Run("creates separate gauges for different registries", func(t *testing.T) {
		resetMaxLagForTest()

		registry1 := solomon.NewRegistry(solomon.NewRegistryOpts())
		registry2 := solomon.NewRegistry(solomon.NewRegistryOpts())

		gauge1 := getOrCreateMaxLagGauge(registry1)
		gauge2 := getOrCreateMaxLagGauge(registry2)

		require.NotEqual(t, gauge1, gauge2, "should create different gauges for different registries")
		require.Len(t, maxLagGaugeRegistry, 2)
	})
}

func TestMaxLagGaugeFunction(t *testing.T) {
	resetMaxLagForTest()

	registry := solomon.NewRegistry(solomon.NewRegistryOpts())
	stats := NewWrapperStats(registry)

	// Store a lag value
	testLag := 7 * time.Second
	stats.storeMaxLag(testLag)

	// The gauge function should read and reset the value
	gaugeFunc := stats.MaxLag.Function()
	require.NotNil(t, gaugeFunc, "FuncGauge should provide a callable function")

	gaugeValue := gaugeFunc()
	expectedValue := float64(testLag) / float64(time.Second)
	require.InDelta(t, expectedValue, gaugeValue, 0.001, "gauge should return lag in seconds")

	// After reading, the value should be reset
	require.Equal(t, uint64(0), maxLagValue.Load(), "reading gauge should reset maxLagValue")

	// Reading again should return 0
	gaugeValue = gaugeFunc()
	require.Equal(t, 0.0, gaugeValue, "gauge should return 0 after reset")
}

func TestWrapperStatsLog(t *testing.T) {
	resetMaxLagForTest()

	registry := solomon.NewRegistry(solomon.NewRegistryOpts())
	stats := NewWrapperStats(registry)

	toNS := func(t time.Time) uint64 {
		return uint64(t.UnixNano())
	}

	// Create test data with old commit time to simulate lag
	oldTime := time.Now().Add(-10 * time.Second)
	input := []abstract.ChangeItem{
		{
			Kind:       abstract.InsertKind,
			CommitTime: toNS(oldTime),
			Table:      "test_table",
			Size:       changeitem.EventSize{Read: 1, Values: 1},
		},
	}

	// Log should store the max lag
	stats.Log(logger.Log, time.Now(), input, false)

	// MaxLag should be set (approximately 10 seconds)
	stored := maxLagValue.Load()
	require.Greater(t, stored, uint64(9*time.Second.Nanoseconds()), "should store lag of approximately 10 seconds")
	require.Less(t, stored, uint64(11*time.Second.Nanoseconds()), "should store lag of approximately 10 seconds")
}

func TestNewWrapperStatsMultipleCalls(t *testing.T) {
	resetMaxLagForTest()

	// Create multiple WrapperStats with the same registry
	// This should not panic due to duplicate registration
	registry := solomon.NewRegistry(solomon.NewRegistryOpts())

	stats1 := NewWrapperStats(registry)
	require.NotNil(t, stats1)
	require.NotNil(t, stats1.MaxLag)

	stats2 := NewWrapperStats(registry)
	require.NotNil(t, stats2)
	require.NotNil(t, stats2.MaxLag)

	// Both should use the same FuncGauge instance
	require.Equal(t, stats1.MaxLag, stats2.MaxLag, "should reuse the same FuncGauge")
}

func TestMaxLagWithParallelSinkers(t *testing.T) {
	resetMaxLagForTest()

	// Simulate parallel sinkers
	registry := solomon.NewRegistry(solomon.NewRegistryOpts())

	numSinkers := 10
	stats := make([]*WrapperStats, numSinkers)
	for i := 0; i < numSinkers; i++ {
		stats[i] = NewWrapperStats(registry)
	}

	var wg sync.WaitGroup
	wg.Add(numSinkers)

	// Each sinker processes batches with different lags concurrently
	for i := 0; i < numSinkers; i++ {
		go func(idx int) {
			defer wg.Done()
			lag := time.Duration(idx+1) * time.Second
			stats[idx].storeMaxLag(lag)
		}(i)
	}

	wg.Wait()

	// The global max lag should be the highest (10 seconds)
	stored := maxLagValue.Load()
	expected := uint64((10 * time.Second).Nanoseconds())
	require.Equal(t, expected, stored, "should store the maximum lag across all parallel sinkers")
}

func TestRegisterNonHashableRegistryPanic(t *testing.T) {
	type NonHashableRegistry struct {
		metrics.Registry
		nonHashable []int
	}

	defer func() {
		if r := recover(); r != nil {
			t.Log("panic occurred:", r)
			t.Fail()
		}
	}()
	_ = NewWrapperStats(&NonHashableRegistry{
		Registry:    solomon.NewRegistry(solomon.NewRegistryOpts()),
		nonHashable: []int{1, 2, 3},
	})
}

func resetMaxLagForTest() {
	// Clear global state before test
	maxLagValue.Store(0)
	maxLagGaugeMutex.Lock()
	maxLagGaugeRegistry = make(map[uintptr]metrics.FuncGauge)
	maxLagGaugeMutex.Unlock()
}
