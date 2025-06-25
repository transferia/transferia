package tasks

import (
	"context"
	"sync"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

const MaxTableStatCount = 1000

type etaParams struct {
	totalETA   float64
	tablesETAs map[string]float64
}

type SnapshotTableMetricsTracker struct {
	ctx             context.Context
	cancel          context.CancelFunc
	pushTicker      *time.Ticker
	waitForComplete sync.WaitGroup
	closeOnce       sync.Once

	transfer     *model.Transfer
	registry     metrics.Registry
	totalETA     float64
	tablesETAs   map[string]float64
	totalGauge   metrics.Gauge
	tablesGauges map[string]metrics.Gauge

	sharded bool

	// For non-sharded snapshot
	parts               []*model.OperationTablePart
	progressUpdateMutex *sync.Mutex

	// For sharded snapshot
	operationID string
	cpClient    coordinator.Coordinator
}

func NewNotShardedSnapshotTableMetricsTracker(
	ctx context.Context,
	transfer *model.Transfer,
	registry metrics.Registry,
	parts []*model.OperationTablePart,
	progressUpdateMutex *sync.Mutex,
) *SnapshotTableMetricsTracker {
	ctx, cancel := context.WithCancel(ctx)
	tracker := &SnapshotTableMetricsTracker{
		ctx:             ctx,
		cancel:          cancel,
		pushTicker:      nil,
		waitForComplete: sync.WaitGroup{},
		closeOnce:       sync.Once{},

		sharded: false,

		transfer:     transfer,
		registry:     registry,
		totalETA:     0,
		tablesETAs:   map[string]float64{},
		totalGauge:   nil,
		tablesGauges: map[string]metrics.Gauge{},

		parts:               parts,
		progressUpdateMutex: progressUpdateMutex,

		operationID: "",
		cpClient:    nil,
	}

	// TODO: TM-8654: Add passing of initParams to constructor.
	tracker.init(nil)

	tracker.waitForComplete.Add(1)
	tracker.pushTicker = time.NewTicker(time.Second * 15)
	go tracker.run()

	return tracker
}

func NewShardedSnapshotTableMetricsTracker(
	ctx context.Context,
	transfer *model.Transfer,
	registry metrics.Registry,
	operationID string,
	cpClient coordinator.Coordinator,
) *SnapshotTableMetricsTracker {
	ctx, cancel := context.WithCancel(ctx)
	tracker := &SnapshotTableMetricsTracker{
		ctx:             ctx,
		cancel:          cancel,
		pushTicker:      nil,
		waitForComplete: sync.WaitGroup{},
		closeOnce:       sync.Once{},

		sharded: true,

		transfer:     transfer,
		registry:     registry,
		totalETA:     0,
		tablesETAs:   map[string]float64{},
		totalGauge:   nil,
		tablesGauges: map[string]metrics.Gauge{},

		parts:               nil,
		progressUpdateMutex: nil,

		operationID: operationID,
		cpClient:    cpClient,
	}

	// TODO: TM-8654: Add passing of initParams to constructor.
	tracker.init(nil)

	tracker.waitForComplete.Add(1)
	tracker.pushTicker = time.NewTicker(time.Second * 15)
	go tracker.run()

	return tracker
}

func (t *SnapshotTableMetricsTracker) run() {
	defer t.waitForComplete.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-t.pushTicker.C:
			t.setMetrics()
		}
	}
}

// Close is thread-safe and could be called many times (only first call matters).
func (t *SnapshotTableMetricsTracker) Close() {
	t.closeOnce.Do(func() {
		t.pushTicker.Stop()
		t.cancel()
		t.waitForComplete.Wait()
		t.setMetrics()
	})
}

// TODO: TM-8654.
// func (t *SnapshotTableMetricsTracker) appendPartsNotSharded(parts []*model.OperationTablePart) {
// 	t.progressUpdateMutex.Lock()
// 	defer t.progressUpdateMutex.Unlock()
// 	t.parts = append(t.parts, parts...)
// }

func (t *SnapshotTableMetricsTracker) getTablesPartsNotSharded() []*model.OperationTablePart {
	t.progressUpdateMutex.Lock()
	defer t.progressUpdateMutex.Unlock()
	partsCopy := make([]*model.OperationTablePart, 0, len(t.parts))
	for _, table := range t.parts {
		partsCopy = append(partsCopy, table.Copy())
	}
	return partsCopy
}

func (t *SnapshotTableMetricsTracker) getTablesParts() []*model.OperationTablePart {
	if t.sharded {
		parts, err := t.cpClient.GetOperationTablesParts(t.operationID)
		if err != nil {
			logger.Log.Error("Failed to get tables for update metrics", log.Error(err))
			return nil
		}
		return parts
	}

	return t.getTablesPartsNotSharded()
}

func (t *SnapshotTableMetricsTracker) calculateETAs() {
	parts := t.getTablesParts()
	for _, part := range parts {
		t.totalETA += float64(part.ETARows)
		tableKey := part.TableFQTN()
		if _, ok := t.tablesETAs[tableKey]; ok {
			t.tablesETAs[tableKey] += float64(part.ETARows)
		} else if len(t.tablesETAs) < MaxTableStatCount {
			t.tablesETAs[tableKey] = float64(part.ETARows)
		}
	}
}

func (t *SnapshotTableMetricsTracker) assignETAs(initParams etaParams) {
	t.totalETA = initParams.totalETA
	if len(initParams.tablesETAs) < MaxTableStatCount {
		t.tablesETAs = initParams.tablesETAs
		return
	}
	// Move only `MaxTableStatCount` elements from `initParams.tablesETAs`.
	for key, value := range initParams.tablesETAs {
		if len(t.tablesETAs) >= MaxTableStatCount {
			break
		}
		t.tablesETAs[key] = value
	}
}

// init uses initParams if passed, otherwise ETAs calculates from parts list.
func (t *SnapshotTableMetricsTracker) init(initParams *etaParams) {
	if initParams != nil {
		t.assignETAs(*initParams)
	} else {
		t.calculateETAs()
	}

	t.totalGauge = t.registry.Gauge("task.snapshot.reminder.total")
	t.totalGauge.Set(t.totalETA)

	for tableKey, tableETA := range t.tablesETAs {
		gauge := t.registry.WithTags(map[string]string{
			"table": tableKey,
		}).Gauge("task.snapshot.remainder.table")
		gauge.Set(tableETA)
		t.tablesGauges[tableKey] = gauge
	}
}

func (t *SnapshotTableMetricsTracker) setMetrics() {
	parts := t.getTablesParts()
	if len(parts) <= 0 {
		return
	}

	totalCompleted := float64(0)
	tablesCompleted := map[string]float64{}
	for _, part := range parts {
		totalCompleted += float64(part.CompletedRows)

		tableKey := part.TableFQTN()
		if _, ok := t.tablesETAs[tableKey]; ok {
			tablesCompleted[tableKey] += float64(part.CompletedRows)
		}
	}

	t.totalGauge.Set(t.totalETA - totalCompleted)

	for tableKey, tableGauge := range t.tablesGauges {
		if eta, ok := t.tablesETAs[tableKey]; ok {
			if completed, ok := tablesCompleted[tableKey]; ok {
				tableGauge.Set(eta - completed)
			}
		}
	}
}
