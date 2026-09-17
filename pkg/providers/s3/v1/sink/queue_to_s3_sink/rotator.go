package queue_to_s3_sink

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
)

type DefaultRotator struct {
	cfg           *s3_v1_model.DefaultRotatorConfig
	partitioner   Partitioner
	timeExtractor TimeExtractor
	nextRotate    time.Time

	// Directory of the currently open file. An item landing in a different directory
	// has to start a new file even when the rotation interval has not elapsed yet, so
	// that e.g. a February 12th record never ends up under the February 11th directory.
	currentDir string

	// Number of records already written to the currently open file. Used when
	// MaxRecordsCount is positive to rotate alongside the time-based strategy.
	recordsInFile int
}

func (r *DefaultRotator) ResetState(item *abstract.ChangeItem) error {
	newTime, err := r.timeExtractor.Extract(item)
	if err != nil {
		return xerrors.Errorf("unable to resolve time of the next file: %w", err)
	}

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

	r.recordsInFile = 0
	return nil
}

func (r *DefaultRotator) ShouldRotate(items []abstract.ChangeItem) (bool, error) {
	lastItem := &items[len(items)-1]

	itemTime, err := r.timeExtractor.Extract(lastItem)
	if err != nil {
		return false, xerrors.Errorf("unable to resolve time of the item: %w", err)
	}
	if !itemTime.Before(r.nextRotate) {
		return true, nil
	}

	// For time based partitioner we need to ensure that record is still consistent with current partition path
	dir, err := r.partitioner.Dir(lastItem)
	if err != nil {
		return false, xerrors.Errorf("unable to resolve directory of the item: %w", err)
	}
	if dir != r.currentDir {
		return true, nil
	}

	if r.cfg.MaxRecordsCount > 0 && r.recordsInFile+len(items) > r.cfg.MaxRecordsCount {
		return true, nil
	}
	return false, nil
}

func (r *DefaultRotator) UpdateState(items []abstract.ChangeItem) {
	if r.cfg.MaxRecordsCount > 0 {
		r.recordsInFile += len(items)
	}
}

func NewDefaultRotator(cfg *s3_v1_model.S3Destination, partitioner Partitioner) (*DefaultRotator, error) {
	switch t := cfg.GetRotator().(type) {
	case *s3_v1_model.DefaultRotatorConfig:
		return &DefaultRotator{
			cfg:           t,
			partitioner:   partitioner,
			timeExtractor: NewTimeExtractor(cfg.GetTimeExtractor()),
			nextRotate:    time.Time{},
			currentDir:    "",
			recordsInFile: 0,
		}, nil
	default:
		return nil, xerrors.Errorf("unexpected rotator config of type %T", t)
	}
}
