package stats

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	dt_metrics "github.com/transferia/transferia/internal/metrics"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
)

const (
	maxLagMetric     = "sinker_pusher_time_row_max_lag_sec"
	maxReadLagMetric = "sinker_pusher_time_row_max_read_lag_sec"
)

type lagRegistryFactory struct {
	name string
	make func() (core_metrics.Registry, func() (map[string][]float64, error))
}

func lagRegistries() []lagRegistryFactory {
	return []lagRegistryFactory{
		{
			name: "solomon",
			make: func() (core_metrics.Registry, func() (map[string][]float64, error)) {
				registry := solomon.NewRegistry(nil)
				return registry, func() (map[string][]float64, error) {
					metrics, err := registry.Gather()
					if err != nil {
						return nil, err
					}
					result := make(map[string][]float64)
					for _, metric := range metrics.List() {
						name := strings.ReplaceAll(metric.Name(), ".", "_")
						if strings.Contains(name, "sinker_pusher_time_row_max_") {
							result[name] = append(result[name], metric.Value().(float64))
						}
					}
					return result, nil
				}
			},
		},
		{
			name: "prometheus",
			make: func() (core_metrics.Registry, func() (map[string][]float64, error)) {
				registry, processed := dt_metrics.NewPrometheusRegistryWithNameProcessor()
				return processed, func() (map[string][]float64, error) {
					metrics, err := registry.Gather()
					if err != nil {
						return nil, err
					}
					result := make(map[string][]float64)
					for _, family := range metrics {
						if strings.Contains(family.GetName(), "sinker_pusher_time_row_max_") {
							for _, metric := range family.GetMetric() {
								result[family.GetName()] = append(result[family.GetName()], metric.GetGauge().GetValue())
							}
						}
					}
					return result, nil
				}
			},
		},
	}
}

type registryWithoutInitialValue struct {
	core_metrics.Registry
}

func TestWrapperStatsRejectsUnsupportedRegistry(t *testing.T) {
	registry := solomon.NewRegistry(nil)
	unsupported := &registryWithoutInitialValue{Registry: registry}
	require.PanicsWithValue(t, "metrics: registry *stats.registryWithoutInitialValue does not support GaugeWithInitialValue", func() {
		NewWrapperStats(unsupported)
	})
	snapshot, err := registry.Gather()
	require.NoError(t, err)
	require.Empty(t, snapshot.List())
}

func TestWrapperStatsLazyLag(t *testing.T) {
	for _, factory := range lagRegistries() {
		t.Run(factory.name, func(t *testing.T) {
			registry, gather := factory.make()
			stats := NewWrapperStats(registry)
			values, err := gather()
			require.NoError(t, err)
			require.Empty(t, values)

			nonRows := []abstract.ChangeItem{{Kind: abstract.DDLKind}}
			stats.LogMaxReadLag(logger.Log, nonRows)
			stats.Log(logger.Log, time.Now(), nonRows, true)
			values, err = gather()
			require.NoError(t, err)
			require.Empty(t, values)

			items := []abstract.ChangeItem{{Kind: abstract.InsertKind, CommitTime: uint64(time.Now().Add(-time.Minute).UnixNano())}}
			stats.LogMaxReadLag(logger.Log, items)
			values, err = gather()
			require.NoError(t, err)
			require.NotContains(t, values, maxLagMetric)
			require.Len(t, values[maxReadLagMetric], 1)
			require.GreaterOrEqual(t, values[maxReadLagMetric][0], 60.0)

			stats.Log(logger.Log, time.Now(), items, true)
			values, err = gather()
			require.NoError(t, err)
			require.Len(t, values[maxLagMetric], 1)
			require.GreaterOrEqual(t, values[maxLagMetric][0], 60.0)

			stats.SetMaxReadLag(17)
			stats.SetMaxLag(23)
			values, err = gather()
			require.NoError(t, err)
			require.Equal(t, []float64{17}, values[maxReadLagMetric])
			require.Equal(t, []float64{23}, values[maxLagMetric])

			stats.SetMaxLag(0)
			values, err = gather()
			require.NoError(t, err)
			require.Equal(t, []float64{0}, values[maxLagMetric])
			require.Equal(t, []float64{17}, values[maxReadLagMetric])

			syncItems := []abstract.ChangeItem{abstract.MakeSynchronizeEvent()}
			syncItems[0].CommitTime = uint64(time.Now().Add(-time.Second).UnixNano())
			stats.LogMaxReadLag(logger.Log, syncItems)
			stats.Log(logger.Log, time.Now(), syncItems, true)
			values, err = gather()
			require.NoError(t, err)
			for _, name := range []string{maxLagMetric, maxReadLagMetric} {
				require.Greater(t, values[name][0], 0.0)
				require.Less(t, values[name][0], 5.0)
			}
		})
	}
}

func TestWrapperStatsSharedAcrossPipelines(t *testing.T) {
	for _, factory := range lagRegistries() {
		t.Run(factory.name, func(t *testing.T) {
			registry, gather := factory.make()
			first := NewWrapperStats(registry)
			first.SetMaxLag(11)
			first.SetMaxReadLag(13)
			retry := NewWrapperStats(registry)
			require.NotSame(t, first, retry)
			values, err := gather()
			require.NoError(t, err)
			require.Equal(t, []float64{11}, values[maxLagMetric])
			require.Equal(t, []float64{13}, values[maxReadLagMetric])
			retry.SetMaxLag(29)
			retry.SetMaxReadLag(31)
			values, err = gather()
			require.NoError(t, err)
			require.Equal(t, []float64{29}, values[maxLagMetric])
			require.Equal(t, []float64{31}, values[maxReadLagMetric])
			first.SetMaxLag(43)
			first.SetMaxReadLag(47)
			values, err = gather()
			require.NoError(t, err)
			require.Equal(t, []float64{43}, values[maxLagMetric])
			require.Equal(t, []float64{47}, values[maxReadLagMetric])

			firstScope := registry.WithPrefix("scope").WithTags(map[string]string{"job_index": "1", "resource_id": "transfer"})
			equivalentScope := registry.WithTags(map[string]string{"resource_id": "transfer"}).WithTags(map[string]string{"job_index": "1"}).WithPrefix("scope")
			scoped := NewWrapperStats(firstScope)
			scoped.SetMaxLag(37)
			scopedRetry := NewWrapperStats(equivalentScope)
			scopedRetry.SetMaxLag(39)
			other := NewWrapperStats(firstScope.WithTags(map[string]string{"job_index": "2"}))
			require.NotSame(t, scoped, other)
			other.SetMaxLag(41)
			values, err = gather()
			require.NoError(t, err)
			require.ElementsMatch(t, []float64{39, 41}, values["scope_"+maxLagMetric])
		})
	}
}

func TestWrapperStatsConcurrentLagUpdates(t *testing.T) {
	for _, factory := range lagRegistries() {
		t.Run(factory.name, func(t *testing.T) {
			registry, gather := factory.make()
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					stats := NewWrapperStats(registry)
					for value := 1; value <= 100; value++ {
						stats.SetMaxLag(float64(value))
						stats.SetMaxReadLag(float64(value))
					}
				}()
			}
			close(start)
			for i := 0; i < 100; i++ {
				values, err := gather()
				require.NoError(t, err)
				for _, samples := range values {
					require.Len(t, samples, 1)
					require.Greater(t, samples[0], 0.0)
				}
			}
			wg.Wait()
			values, err := gather()
			require.NoError(t, err)
			require.Equal(t, []float64{100}, values[maxLagMetric])
			require.Equal(t, []float64{100}, values[maxReadLagMetric])
		})
	}
}
