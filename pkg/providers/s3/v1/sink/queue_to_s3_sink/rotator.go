package queue_to_s3_sink

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
)

type Rotator interface {
	ShouldRotate(item *abstract.ChangeItem) bool
	UpdateState(item *abstract.ChangeItem) error
}

type DefaultRotator struct {
	cfg        *s3_v1_model.DefaultRotatorConfig
	nextRotate time.Time
}

var _ Rotator = (*DefaultRotator)(nil)

func (r *DefaultRotator) UpdateState(item *abstract.ChangeItem) error {
	newTime := r.getRawMessageWriteTime(item)

	if r.nextRotate.IsZero() {
		r.nextRotate = newTime.Add(r.cfg.Interval)
		return nil
	}
	r.nextRotate = r.nextRotate.Add(r.cfg.Interval)
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

func NewRotator(cfg s3_v1_model.RotatorConfig) Rotator {
	switch t := cfg.(type) {
	case *s3_v1_model.DefaultRotatorConfig:
		return &DefaultRotator{
			cfg:        t,
			nextRotate: time.Time{},
		}
	default:
		return nil
	}
}
