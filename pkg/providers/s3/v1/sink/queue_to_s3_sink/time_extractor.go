package queue_to_s3_sink

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
)

// TimeExtractor supplies the timestamp that drives file rotation and, for the time based
// partitioner, the time bucket an item is laid out under. Rotator and Partitioner must
// agree on it, otherwise a file could be rolled on one clock and named on another.
type TimeExtractor interface {
	// Extract returns the time of the item. Implementations that do not read the item at
	// all accept nil.
	Extract(item *abstract.ChangeItem) (time.Time, error)
}

var _ TimeExtractor = (*RecordMetaTimeExtractor)(nil)

// RecordMetaTimeExtractor reports the queue write time carried in the record's metadata
type RecordMetaTimeExtractor struct{}

func (e *RecordMetaTimeExtractor) Extract(item *abstract.ChangeItem) (time.Time, error) {
	if item == nil {
		return time.Time{}, xerrors.New("unable to extract time from a nil change item")
	}
	idx := abstract.RawDataColsIDX[abstract.RawMessageWriteTime]
	if idx >= len(item.ColumnValues) {
		return time.Time{}, xerrors.Errorf("column with kafka write time is not present, change item: %v", item)
	}
	switch v := item.ColumnValues[idx].(type) {
	case time.Time:
		return v, nil
	default:
		return time.Time{}, xerrors.Errorf("unexpected time value of type %T: %v", v, v)
	}
}

var _ TimeExtractor = (*DataFieldTimeExtractor)(nil)

// DataFieldTimeExtractor reports the time held in a column of the record itself. A record
// without a usable value in that column fails the push instead of falling back to the
// current time: silently mixing wallclock into a data derived layout would scatter events
// across the wrong buckets with nothing to show for it afterwards.
type DataFieldTimeExtractor struct {
	column string
}

func (e *DataFieldTimeExtractor) Extract(item *abstract.ChangeItem) (time.Time, error) {
	if item == nil {
		return time.Time{}, xerrors.New("unable to extract time from a nil change item")
	}
	extracted, err := model.ExtractTimeColStrict(*item, e.column)
	if err != nil {
		return time.Time{}, xerrors.Errorf("unable to extract time from the record: %w", err)
	}
	return extracted, nil
}

func NewTimeExtractor(cfg s3_v1_model.TimeExtractorConfig) TimeExtractor {
	switch t := cfg.(type) {
	case *s3_v1_model.RecordMetaTimeExtractorConfig:
		return &RecordMetaTimeExtractor{}
	case *s3_v1_model.DataFieldTimeExtractorConfig:
		return &DataFieldTimeExtractor{column: t.Column}
	default:
		return nil
	}
}
