package stats

import (
	"strings"
	"sync"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract"
)

type SourceStats struct {
	registry      metrics.Registry
	rw            sync.Mutex
	Usage         metrics.Gauge
	CompressRatio metrics.Gauge
	Read          metrics.Gauge
	Extract       metrics.Gauge
	Master        metrics.Gauge
	DDLError      metrics.Counter
	Error         metrics.Counter
	Fatal         metrics.Counter
	Size          metrics.Counter
	Count         metrics.Counter
	ChangeItems   metrics.Counter
	Parsed        metrics.Counter
	Unparsed      metrics.Counter
	TransformTime metrics.Timer
	DecodeTime    metrics.Timer
	PushTime      metrics.Timer
	DelayTime     metrics.Timer
	tableStat     map[string]*TableStat
}

func (s *SourceStats) SourceRows(table string, rows uint64) {
	if abstract.IsSystemTable(table) {
		return
	}
	s.rw.Lock()
	defer s.rw.Unlock()
	table = strings.ReplaceAll(table, "public", "")
	table = strings.ReplaceAll(table, ".", "_")
	table = strings.ReplaceAll(table, "\"", "")
	diff := sourceRows(table, rows)
	if _, ok := s.tableStat[table]; !ok {
		s.tableStat[table] = NewTableMetrics(s.registry, table)
	}
	s.tableStat[table].Source.Set(float64(rows))
	if diff > 0 {
		s.tableStat[table].Diff.Set(100 * diff)
	}
}

func (s *SourceStats) WithTags(tags map[string]string) *SourceStats {
	return NewSourceStats(s.registry.WithTags(tags))
}

func NewSourceStats(registry metrics.Registry) *SourceStats {
	return &SourceStats{
		registry:      registry,
		rw:            sync.Mutex{},
		tableStat:     map[string]*TableStat{},
		DDLError:      registry.Counter("publisher.consumer.ddl_error"),
		Error:         registry.Counter("publisher.consumer.error"),
		Fatal:         registry.Counter("publisher.consumer.fatal"),
		Usage:         registry.Gauge("publisher.consumer.log_usage_bytes"),
		Read:          registry.Gauge("publisher.consumer.read_bytes"),
		CompressRatio: registry.Gauge("publisher.consumer.compress_ratio"),
		Extract:       registry.Gauge("publisher.consumer.extracted_bytes"),
		Master:        registry.Gauge("publisher.consumer.active"),
		Size:          registry.Counter("publisher.data.bytes"),
		Count:         registry.Counter("publisher.data.transactions"),
		ChangeItems:   registry.Counter("publisher.data.changeitems"),
		Parsed:        registry.Counter("publisher.data.parsed_rows"),
		Unparsed:      registry.Counter("publisher.data.unparsed_rows"),
		TransformTime: registry.Timer("publisher.time.transform_ms"),
		DecodeTime:    registry.Timer("publisher.time.parse_ms"),
		PushTime:      registry.Timer("publisher.time.push_ms"),
		DelayTime:     registry.Timer("publisher.time.delay_ms"),
	}
}
