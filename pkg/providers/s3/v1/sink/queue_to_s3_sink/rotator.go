package queue_to_s3_sink

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
)

type Rotator interface {
	ShouldRotate(item *abstract.ChangeItem) (bool, error)
	UpdateState(item *abstract.ChangeItem) error
}

type DefaultRotator struct {
	cfg         *s3_v1_model.DefaultRotatorConfig
	partitioner Partitioner
	nextRotate  time.Time

	// Directory of the currently open file. An item landing in a different directory
	// has to start a new file even when the rotation interval has not elapsed yet, so
	// that e.g. a February 12th record never ends up under the February 11th directory.
	currentDir string
}

var _ Rotator = (*DefaultRotator)(nil)

func (r *DefaultRotator) UpdateState(item *abstract.ChangeItem) error {
	newTime := rawMessageWriteTime(item)

	// If time frames between messages is huge we can spend a lot of time just reapdating rotator wich is useless
	if newTime.Sub(r.nextRotate) >= r.cfg.Interval {
		r.nextRotate = newTime.Add(r.cfg.Interval)
	} else {
		r.nextRotate = r.nextRotate.Add(r.cfg.Interval)
	}

	dir, err := r.partitioner.Dir(item)
	if err != nil {
		return xerrors.Errorf("unable to resolve directory of the next file: %w", err)
	}
	r.currentDir = dir
	return nil
}

func (r *DefaultRotator) ShouldRotate(item *abstract.ChangeItem) (bool, error) {
	if !rawMessageWriteTime(item).Before(r.nextRotate) {
		return true, nil
	}

	dir, err := r.partitioner.Dir(item)
	if err != nil {
		return false, xerrors.Errorf("unable to resolve directory of the item: %w", err)
	}
	return dir != r.currentDir, nil
}

// rawMessageWriteTime reads the queue write time off a mirror-shaped ChangeItem.
// Shared by Rotator (for rotation timing) and TimeBasedPartitioner (for path bucketing).
func rawMessageWriteTime(item *abstract.ChangeItem) time.Time {
	switch v := item.ColumnValues[abstract.RawDataColsIDX[abstract.RawMessageWriteTime]].(type) {
	case time.Time:
		return v
	default:
		return time.Time{}
	}
}

func NewRotator(cfg s3_v1_model.RotatorConfig, partitioner Partitioner) Rotator {
	switch t := cfg.(type) {
	case *s3_v1_model.DefaultRotatorConfig:
		return &DefaultRotator{
			cfg:         t,
			partitioner: partitioner,
			nextRotate:  time.Time{},
			currentDir:  "",
		}
	default:
		return nil
	}
}
