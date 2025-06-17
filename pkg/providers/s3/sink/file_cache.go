//go:build !disable_s3_provider

package sink

import (
	"math"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	mathutil "github.com/transferia/transferia/pkg/util/math"
)

type FileCache struct {
	tableID         abstract.TableID
	items           []*abstract.ChangeItem
	approximateSize uint64
	minLSN          uint64
	maxLSN          uint64
}

func (f *FileCache) Add(item *abstract.ChangeItem) error {
	if f.tableID != item.TableID() {
		return xerrors.Errorf("FileCache:items with different table ids in the same s3 file. cache table id: %v, item table id: %v", f.tableID, item.TableID())
	}
	f.items = append(f.items, item)
	f.approximateSize += getSize(item)
	f.minLSN = mathutil.MinT(item.LSN, f.minLSN)
	f.maxLSN = mathutil.MaxT(item.LSN, f.maxLSN)
	return nil
}

// extra copy, but works fine with range, useful for tests
func (f *FileCache) AddCopy(item abstract.ChangeItem) error {
	return f.Add(&item)
}

// Split file cache into file cache parts. Each cache part
// has items that contain in one of then given intervals
// and with consecutive LSNs and size that le than maxCacheSize
// NB intervals range is expected to be sorted
func (f *FileCache) Split(intervals []ObjectRange, maxCacheSize uint64) []*FileCache {
	parts := make([]*FileCache, 0)
	if len(intervals) == 0 {
		return parts
	}

	itemIdx, intervalIdx := 0, 0

	for itemIdx < len(f.items) && intervalIdx < len(intervals) {
		// first for is not used in this pkg cos given interval is already in min/max lsn range, it is here for safety
		for intervalIdx < len(intervals) && f.items[itemIdx].LSN > intervals[intervalIdx].To {
			intervalIdx++
		}
		if intervalIdx == len(intervals) {
			break
		}
		for itemIdx < len(f.items) && f.items[itemIdx].LSN < intervals[intervalIdx].From {
			itemIdx++
		}
		if itemIdx == len(f.items) {
			break
		}

		consecutiveIntervals := intervalIdx > 0 && intervals[intervalIdx-1].To+1 == intervals[intervalIdx].From
		if !consecutiveIntervals {
			parts = append(parts, newFileCache(f.tableID))
		}

		for itemIdx < len(f.items) && f.items[itemIdx].LSN <= intervals[intervalIdx].To {
			if !parts[len(parts)-1].Empty() && parts[len(parts)-1].approximateSize+getSize(f.items[itemIdx]) > maxCacheSize {
				parts = append(parts, newFileCache(f.tableID))
			}
			lastPart := parts[len(parts)-1]
			_ = lastPart.Add(f.items[itemIdx])
			itemIdx++
		}
		intervalIdx++
	}

	return parts
}

func (f *FileCache) Clear() {
	for j := 0; j < len(f.items); j++ {
		f.items[j] = nil
	}
	f.items = make([]*abstract.ChangeItem, 0)
	f.minLSN = math.MaxUint64
	f.maxLSN = 0
}

func (f *FileCache) ExtractLsns() []uint64 {
	lsns := make([]uint64, 0)
	for _, item := range f.items {
		lsns = append(lsns, item.LSN)
	}
	return lsns
}

func (f *FileCache) LSNRange() (uint64, uint64) {
	return f.minLSN, f.maxLSN
}

func (f *FileCache) Empty() bool {
	return len(f.items) == 0
}

func (f *FileCache) IsSnapshotFileCache() bool {
	return f.minLSN == 0 && f.maxLSN == 0 && len(f.items) > 0
}

func newFileCache(tableID abstract.TableID) *FileCache {
	return &FileCache{
		tableID:         tableID,
		items:           make([]*abstract.ChangeItem, 0),
		approximateSize: 0,
		minLSN:          math.MaxUint64,
		maxLSN:          0,
	}
}

func getSize(item *abstract.ChangeItem) uint64 {
	if item.Size.Values > 0 {
		return item.Size.Values
	}
	return item.Size.Read
}
