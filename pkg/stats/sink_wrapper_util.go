package stats

import (
	"time"

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
