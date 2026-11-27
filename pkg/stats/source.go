package stats

import (
	"strings"
	"sync"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/pkg/abstract"
)

type SourceStats struct {
	attempt       *ReplicationAttemptMetrics
	registry      core_metrics.Registry
	rw            sync.Mutex
	Usage         core_metrics.Gauge
	CompressRatio core_metrics.Gauge
	Read          core_metrics.Gauge
	Extract       core_metrics.Gauge
	Master        core_metrics.Gauge
	DDLError      core_metrics.Counter
	Error         core_metrics.Counter
	Fatal         core_metrics.Counter
	Size          core_metrics.Counter
	Count         core_metrics.Counter
	ChangeItems   core_metrics.Counter
	Parsed        core_metrics.Counter
	Unparsed      core_metrics.Counter
	TransformTime core_metrics.Timer
	DecodeTime    core_metrics.Timer
	PushTime      core_metrics.Timer
	DelayTime     core_metrics.Timer
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
	return s.attempt.BuildSourceStats(s.registry.WithTags(tags))
}

// RegisteredSourceStats owns the metrics associated with one source instance.
type RegisteredSourceStats struct {
	*SourceStats
}

func (s *SourceStats) BuildRegisteredSourceStats() RegisteredSourceStats {
	return RegisteredSourceStats{SourceStats: s}
}

func (s *SourceStats) Close() {}

func NewSourceStats(registry core_metrics.Registry) *SourceStats {
	return &SourceStats{
		attempt:       nil,
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
