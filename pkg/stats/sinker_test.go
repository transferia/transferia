package stats

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
)

func TestSinkerStatsTableSharedAcrossSinks(t *testing.T) {
	registry := solomon.NewRegistry(nil)
	sinks := []*SinkerStats{NewSinkerStats(registry), NewSinkerStats(registry)}
	var wg sync.WaitGroup
	for _, sink := range sinks {
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for m := 0; m < 200; m++ {
					sink.Table("shared_table", fmt.Sprintf("metric_%d", m), 1)
				}
			}()
		}
	}
	wg.Wait()
	stat := NewTableMetrics(registry, "shared_table")
	require.Len(t, stat.Metrics, 200)
}
