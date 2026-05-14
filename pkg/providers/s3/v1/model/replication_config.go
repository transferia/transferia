package s3_model

import (
	"time"
)

type (
	RotatorType     string
	PartitionerType string
)

const (
	DefaultRotator = RotatorType("DEFAULT")

	DefaultPartitioner = PartitionerType("DEFAULT")
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

type PartitionerUnion struct {
	Default *DefaultPartitionerConfig
}
