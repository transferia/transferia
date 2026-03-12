package queue_to_s3_sink

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
)

type DefaultRotator struct {
	interval   time.Duration
	nextRotate time.Time
}

var _ s3_provider.Rotator = (*DefaultRotator)(nil)

func NewDefaultRotator(interval time.Duration) *DefaultRotator {
	return &DefaultRotator{
		interval:   interval,
		nextRotate: time.Time{},
	}
}

func (r *DefaultRotator) UpdateState(item *abstract.ChangeItem) error {
	newTime := r.getRawMessageWriteTime(item)

	if r.nextRotate.IsZero() {
		r.nextRotate = newTime.Add(r.interval)
		return nil
	}
	r.nextRotate = r.nextRotate.Add(r.interval)
	return nil
}

func (r *DefaultRotator) ShouldRotate(item *abstract.ChangeItem) bool {
	commitTime := r.getRawMessageWriteTime(item)
	return !commitTime.Before(r.nextRotate)
}

func (r *DefaultRotator) getRawMessageWriteTime(item *abstract.ChangeItem) time.Time {
	switch v := item.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessageWriteTime]].(type) {
	case time.Time:
		return v
	default:
		return time.Time{}
	}
}
