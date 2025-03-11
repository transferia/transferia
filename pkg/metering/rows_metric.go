package metering

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/base"
	"github.com/transferria/transferria/pkg/util"
)

type RowsMetric struct {
	serializationSchemas []MetricSchema

	statsMu     sync.Mutex
	startTS     time.Time
	rawSizes    Stats
	parsedSizes Stats
	runtimeType string
}

type RowsMetricState struct {
	serializationSchemas []MetricSchema

	rawSizes    Stats
	parsedSizes Stats
	startTS     time.Time
	finishTS    time.Time
	runtimeType string
}

type Stats struct {
	minSize      uint64
	maxSize      uint64
	totalSize    uint64
	totalNumber  uint64
	sizeDivision map[SizeBucket]uint64
}

type SizeBucket uint64

const (
	bucket1Kb   = SizeBucket(1024)
	bucket512Kb = SizeBucket(512 * 1024)
	bucket1Mb   = SizeBucket(1024 * 1024)
	bucket2Mb   = SizeBucket(2 * 1024 * 1024)
	bucket5Mb   = SizeBucket(5 * 1024 * 1024)
	bucket10Mb  = SizeBucket(10 * 1024 * 1024)
	bucketg10Mb = SizeBucket(10*1024*1024 + 1)
)

func (*RowsMetricState) isMetricState() {}

func (*RowsMetric) isMetric() {}

func (rm *RowsMetric) Reset() *RowsMetricState {
	rm.statsMu.Lock()
	prevIntervalFinishTS := time.Now()
	prevState := &RowsMetricState{
		serializationSchemas: rm.serializationSchemas,

		rawSizes:    rm.rawSizes,
		parsedSizes: rm.parsedSizes,
		startTS:     rm.startTS,
		finishTS:    prevIntervalFinishTS,
		runtimeType: rm.runtimeType,
	}
	rm.rawSizes = EmptyStats()
	rm.parsedSizes = EmptyStats()
	rm.startTS = prevIntervalFinishTS
	rm.statsMu.Unlock()
	return prevState
}

func (rm *RowsMetric) Count(items []abstract.ChangeItem, calculateValuesSize bool) {
	rm.statsMu.Lock()
	defer rm.statsMu.Unlock()

	for _, i := range items {
		if i.IsSystemKind() {
			continue
		}
		rm.rawSizes.Add(i.Size.Read)
		if calculateValuesSize && i.Size.Values == 0 {
			rm.parsedSizes.Add(util.DeepSizeof(i.ColumnValues))
		} else {
			rm.parsedSizes.Add(i.Size.Values)
		}
	}
}

func (rm *RowsMetric) CountForBatch(input base.EventBatch) {
	count := uint64(input.Count())
	if count > 0 {
		rm.statsMu.Lock()
		defer rm.statsMu.Unlock()
		avgSizePerItem := uint64(input.Size()) / count
		rm.rawSizes.AddBatch(count, avgSizePerItem)
		rm.parsedSizes.AddBatch(count, avgSizePerItem)
	}
}

func (rms *RowsMetricState) Serialize(baseOpts *MeteringOpts) (map[MetricSchema][]string, error) {
	res := map[MetricSchema][]string{}
	var errors []error
	for _, schema := range rms.serializationSchemas {
		switch schema {
		// for now only output rows have to be serialized
		case OutputRowsCountSchema:
			rows, err := rms.countRowsMetric(baseOpts, OutputRowsCountSchema)
			if err == nil {
				res[schema] = []string{rows}
			} else {
				errors = append(errors, xerrors.Errorf("cannot serialize rows count for %v, transfer_id = %v: %w", schema, baseOpts.TransferID, err))
			}
		case OutputRowsSchema:
			smallRows, err := rms.smallRowsMetric(baseOpts, OutputRowsSchema)
			if err == nil {
				res[schema] = []string{smallRows}
			} else {
				errors = append(errors, xerrors.Errorf("cannot serialize small rows for %v, transfer_id = %v: %w", schema, baseOpts.TransferID, err))
			}

			largeRows, err := rms.largeRowsMetric(baseOpts, OutputRowsSchema)
			if err == nil {
				res[schema] = append(res[schema], largeRows)
			} else {
				errors = append(errors, xerrors.Errorf("cannot serialize large rows for %v, transfer_id = %v: %w", schema, baseOpts.TransferID, err))
			}
		}
	}
	var resErr error
	if len(errors) > 0 {
		resErr = xerrors.Errorf("errors occur while serialization metrics: %v", util.ToString(errors))
	}

	return res, resErr
}

func (rms *RowsMetricState) countRowsMetric(baseOpts *MeteringOpts, schema MetricSchema) (string, error) {
	res := baseOpts.BaseMetricFieldsWithLabels(rms.startTS, rms.finishTS, []string{})
	res["schema"] = schema

	tags := res["tags"].(map[string]interface{})
	tags["preview"] = getPreview(baseOpts)

	res["usage"] = map[string]interface{}{
		"start":    rms.startTS.Unix(),
		"finish":   rms.finishTS.Unix(),
		"type":     "delta",
		"unit":     "rows",
		"quantity": rms.parsedSizes.totalNumber,
	}
	jsonMetric, err := json.Marshal(res)
	return string(jsonMetric), err
}

func (rms *RowsMetricState) smallRowsMetric(baseOpts *MeteringOpts, schema MetricSchema) (string, error) {
	res := baseOpts.BaseMetricFieldsWithTags(rms.startTS, rms.finishTS, []string{"small"})
	res["schema"] = schema
	tags := res["tags"].(map[string]interface{})
	tags["row_size"] = "small"
	tags["runtime"] = rms.runtimeType
	res["usage"] = map[string]interface{}{
		"start":    rms.startTS.Unix(),
		"finish":   rms.finishTS.Unix(),
		"type":     "delta",
		"unit":     "rows",
		"quantity": rms.smallRowsNumber(),
	}
	jsonMetric, err := json.Marshal(res)
	return string(jsonMetric), err
}

func (rms *RowsMetricState) largeRowsMetric(baseOpts *MeteringOpts, schema MetricSchema) (string, error) {
	res := baseOpts.BaseMetricFieldsWithTags(rms.startTS, rms.finishTS, []string{"large"})
	res["schema"] = schema
	tags := res["tags"].(map[string]interface{})
	tags["row_size"] = "large"
	tags["runtime"] = rms.runtimeType
	res["usage"] = map[string]interface{}{
		"start":    rms.startTS.Unix(),
		"finish":   rms.finishTS.Unix(),
		"type":     "delta",
		"unit":     "rows",
		"quantity": rms.largeRowsNumber(),
	}
	jsonMetric, err := json.Marshal(res)
	return string(jsonMetric), err
}

func (rms *RowsMetricState) smallRowsNumber() uint64 {
	totalNumber := uint64(0)
	for size, number := range rms.parsedSizes.sizeDivision {
		if size <= bucket1Mb {
			totalNumber += number
		}
	}
	return totalNumber
}

func (rms *RowsMetricState) largeRowsNumber() uint64 {
	totalNumber := uint64(0)
	for size, number := range rms.parsedSizes.sizeDivision {
		if size > bucket1Mb {
			totalNumber += number
		}
	}
	return totalNumber
}

func EmptyStats() Stats {
	return Stats{
		minSize:      0,
		maxSize:      0,
		totalSize:    0,
		totalNumber:  0,
		sizeDivision: map[SizeBucket]uint64{},
	}
}

func (s *Stats) Add(value uint64) {
	s.addToSizeDivision(value)
	s.updateMin(value)
	s.updateMax(value)
	s.totalNumber += 1
	s.totalSize += value
}

func (s *Stats) AddBatch(count uint64, avgSize uint64) {
	s.addToSizeDivisionBatch(avgSize, count)
	s.updateMin(avgSize)
	s.updateMax(avgSize)
	s.totalNumber += count
	s.totalSize += avgSize * count
}

func (s *Stats) updateMin(v uint64) {
	if s.totalNumber == 0 || v < s.minSize {
		s.minSize = v
	}
}

func (s *Stats) updateMax(v uint64) {
	if v > s.maxSize {
		s.maxSize = v
	}
}

func (s *Stats) addToSizeDivision(size uint64) {
	switch {
	case size <= uint64(bucket1Kb):
		s.sizeDivision[bucket1Kb] += 1
	case size <= uint64(bucket512Kb):
		s.sizeDivision[bucket512Kb] += 1
	case size <= uint64(bucket1Mb):
		s.sizeDivision[bucket1Mb] += 1
	case size <= uint64(bucket2Mb):
		s.sizeDivision[bucket2Mb] += 1
	case size <= uint64(bucket5Mb):
		s.sizeDivision[bucket5Mb] += 1
	case size <= uint64(bucket10Mb):
		s.sizeDivision[bucket10Mb] += 1
	case size > uint64(bucket10Mb):
		s.sizeDivision[bucketg10Mb] += 1
	}
}

func (s *Stats) addToSizeDivisionBatch(approxItemSize uint64, count uint64) {
	switch {
	case approxItemSize <= uint64(bucket1Kb):
		s.sizeDivision[bucket1Kb] += count
	case approxItemSize <= uint64(bucket512Kb):
		s.sizeDivision[bucket512Kb] += count
	case approxItemSize <= uint64(bucket1Mb):
		s.sizeDivision[bucket1Mb] += count
	case approxItemSize <= uint64(bucket2Mb):
		s.sizeDivision[bucket2Mb] += count
	case approxItemSize <= uint64(bucket5Mb):
		s.sizeDivision[bucket5Mb] += count
	case approxItemSize <= uint64(bucket10Mb):
		s.sizeDivision[bucket10Mb] += count
	case approxItemSize > uint64(bucket10Mb):
		s.sizeDivision[bucketg10Mb] += count
	}
}

func getPreview(baseOpts *MeteringOpts) bool {
	preview, tagPresent := baseOpts.Tags["preview"]
	if tagPresent {
		return preview.(bool)
	}
	//should never happen, but may for some freezed transfers
	return true
}

func NewRowsMetric(serializationSchemas []MetricSchema, runtimeType string) *RowsMetric {
	return &RowsMetric{
		serializationSchemas: serializationSchemas,

		statsMu:     sync.Mutex{},
		startTS:     time.Now(),
		rawSizes:    EmptyStats(),
		parsedSizes: EmptyStats(),
		runtimeType: runtimeType,
	}
}
