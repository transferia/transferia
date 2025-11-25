package stats

import (
	"reflect"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

func batchStats(logger log.Logger, input []abstract.ChangeItem) (oldestTime time.Time, freshestTime time.Time, rowEvents int64, bytes uint64) {
	oldestTime = time.Time{}
	freshestTime = time.Time{}
	itemsWithoutCommitTime := 0
	for _, item := range input {
		// condition what we take into account
		if !(item.IsRowEvent() || item.Kind == abstract.SynchronizeKind) {
			continue
		}

		// keep 'rowEvents' & 'bytes'
		if item.IsRowEvent() {
			rowEvents++
		}
		bytes += item.Size.Values

		// determine 'eventTime'
		eventTime := time.Unix(0, int64(item.CommitTime))
		if item.CommitTime == 0 {
			itemsWithoutCommitTime++
			eventTime = time.Now()
		}

		// fill min & max
		if oldestTime.IsZero() && freshestTime.IsZero() {
			// Init oldestTime and freshestTime.
			oldestTime = eventTime
			freshestTime = eventTime
			continue
		}
		if oldestTime.Before(eventTime) {
			oldestTime = eventTime
		}
		if freshestTime.After(eventTime) {
			freshestTime = eventTime
		}
	}
	if itemsWithoutCommitTime != 0 {
		logger.Infof("%d of %d row-items have no CommitTime", itemsWithoutCommitTime, rowEvents)
	}
	return oldestTime, freshestTime, rowEvents, bytes
}

// getRegistryKey returns a unique key for the registry.
// This function requires that the registry is passed as a pointer (e.g., *solomon.Registry).
// Passing a registry by value (struct) or nil will return 0.
func getRegistryKey(registry metrics.Registry) uintptr {
	if registry == nil {
		return 0
	}

	rv := reflect.ValueOf(registry)
	if rv.Kind() == reflect.Ptr {
		return rv.Pointer()
	}

	return 0
}
