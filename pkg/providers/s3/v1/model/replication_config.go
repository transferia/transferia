package s3_model

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/xlocale"
)

type (
	RotatorType     string
	PartitionerType string
)

const (
	DefaultRotator = RotatorType("DEFAULT")

	DefaultPartitioner   = PartitionerType("DEFAULT")
	TimeBasedPartitioner = PartitionerType("TIME_BASED")
)

type RotatorConfig interface {
	IsRotatorConfig()
}

var _ RotatorConfig = (*DefaultRotatorConfig)(nil)

type DefaultRotatorConfig struct {
	Interval time.Duration
}

func (r *DefaultRotatorConfig) IsRotatorConfig() {}

type RotatorUnion struct {
	Default *DefaultRotatorConfig
}

type PartitionerConfig interface {
	IsPartitionerConfig()
}

var _ PartitionerConfig = (*DefaultPartitionerConfig)(nil)

type DefaultPartitionerConfig struct{}

func (p *DefaultPartitionerConfig) IsPartitionerConfig() {}

var _ PartitionerConfig = (*TimeBasedPartitionerConfig)(nil)

// TimeBasedPartitionType is the size of the time bucket the time based partitioner lays files out by
type TimeBasedPartitionType string

const (
	TimePartitionHour  = TimeBasedPartitionType("h")
	TimePartitionDay   = TimeBasedPartitionType("d")
	TimePartitionMonth = TimeBasedPartitionType("m")
)

// Go reference-time layouts rendering the time-bucket path segment of each partition type
const (
	hourPathFormat  = "2006/01/02/15"
	dayPathFormat   = "2006/01/02"
	monthPathFormat = "2006/01"
)

type TimeBasedPartitionerConfig struct {
	// PartitionType of the time bucket, which fully determines the layout of the bucket path segment
	PartitionType TimeBasedPartitionType
	// IANA name of the timezone the time bucket is rendered in, e.g. "Europe/Moscow". Empty means UTC
	Timezone string
}

func (p *TimeBasedPartitionerConfig) IsPartitionerConfig() {}

// PathFormat returns the Go reference-time layout the time bucket path segment is rendered with
func (p *TimeBasedPartitionerConfig) PathFormat() (string, error) {
	switch p.PartitionType {
	case TimePartitionHour:
		return hourPathFormat, nil
	case TimePartitionDay:
		return dayPathFormat, nil
	case TimePartitionMonth:
		return monthPathFormat, nil
	default:
		return "", xerrors.Errorf("unknown partition type %q", p.PartitionType)
	}
}

func (p *TimeBasedPartitionerConfig) Location() (*time.Location, error) {
	if p.Timezone == "" {
		return time.UTC, nil
	}
	return xlocale.Load(p.Timezone)
}

type PartitionerUnion struct {
	Default   *DefaultPartitionerConfig
	TimeBased *TimeBasedPartitionerConfig
}
