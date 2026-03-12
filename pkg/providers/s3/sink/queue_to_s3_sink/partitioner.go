package queue_to_s3_sink

import (
	"strconv"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
)

type Partitioner interface {
	ConstructKey(item *abstract.ChangeItem) (string, error)
}

// Partitioner default <prefix>/<topic>/partition=<kafkaPartition>/<topic>+<kafkaPartition>+<startOffset>.<format>[.gz]
type DefaultPartitioner struct {
	config *PartitionerConfig
}

func (p *DefaultPartitioner) ConstructKey(item *abstract.ChangeItem) (string, error) {
	if item == nil {
		return "", xerrors.Errorf("unable to extract data to construct next file name")
	}

	if !p.config.Initialised() {
		if err := p.config.FinishInit(item); err != nil {
			return "", err
		}
	}

	var builder strings.Builder
	builder.Grow(p.calculateNameLength(item.QueueMessageMeta.Offset))

	if len(p.config.Prefix()) > 0 {
		builder.WriteString(p.config.Prefix())
		builder.WriteByte('/')
	}

	builder.WriteString(p.config.Topic())
	builder.WriteString("/partition=")
	builder.WriteString(strconv.Itoa(p.config.Partition()))
	builder.WriteByte('/')
	builder.WriteString(p.config.Topic())
	builder.WriteByte('+')
	builder.WriteString(strconv.Itoa(p.config.Partition()))
	builder.WriteByte('+')
	builder.WriteString(strconv.FormatUint(item.QueueMessageMeta.Offset, 10))
	builder.WriteByte('.')
	builder.WriteString(p.config.Format())

	if p.config.IsGzip() {
		builder.WriteString(".gz")
	}
	return builder.String(), nil
}

func (p *DefaultPartitioner) calculateNameLength(offset uint64) int {
	var res int
	if len(p.config.Prefix()) > 0 {
		res += len(p.config.Prefix()) + len("/") // Can prefix be empty?
	}

	res += len(p.config.Topic()) + len("/")
	res += len("partition=") + len(strconv.Itoa(p.config.Partition())) + len("/")
	res += len(p.config.Topic()) + len("+") + len(strconv.Itoa(p.config.Partition())) + len("+") + len(strconv.FormatUint(offset, 10))
	res += len(".") + len(p.config.Format())

	if p.config.IsGzip() {
		res += len(".gz")
	}
	return res
}

type PartitionerConfig struct {
	prefix          string
	topic           string
	partition       int
	format          string
	isGzip          bool
	partitionerType s3_provider.PartitionerType
	initialised     bool
}

func (c *PartitionerConfig) Prefix() string                    { return c.prefix }
func (c *PartitionerConfig) Topic() string                     { return c.topic }
func (c *PartitionerConfig) Partition() int                    { return c.partition }
func (c *PartitionerConfig) Format() string                    { return c.format }
func (c *PartitionerConfig) IsGzip() bool                      { return c.isGzip }
func (c *PartitionerConfig) Type() s3_provider.PartitionerType { return c.partitionerType }
func (c *PartitionerConfig) Initialised() bool                 { return c.initialised }

func (c *PartitionerConfig) FinishInit(item *abstract.ChangeItem) error {
	if c.initialised {
		return xerrors.Errorf("Trying to initialise already complete partitioner configuration")
	}

	c.topic = item.QueueMessageMeta.TopicName
	c.partition = item.QueueMessageMeta.PartitionNum

	if c.topic == "" {
		return xerrors.Errorf("failed to extract topic name or partition number from received message, message: %v", item)
	}

	c.initialised = true
	return nil
}

func NewPartitionerConfig(cfg *s3_provider.S3Destination) *PartitionerConfig {
	switch cfg.Partitioner {
	default:
		return &PartitionerConfig{
			prefix:          "", // For now, will also receive it from dst config
			topic:           "", // Extract from first message
			partition:       -1, // Extract from first message
			format:          strings.ToLower(string(cfg.OutputFormat)),
			isGzip:          cfg.OutputEncoding == s3_provider.GzipEncoding,
			partitionerType: s3_provider.DefaultPartitionerType,
			initialised:     false,
		}
	}
}

func PartitionerFactory(config *PartitionerConfig) Partitioner {
	switch config.Type() {
	case s3_provider.DefaultPartitionerType:
		return &DefaultPartitioner{
			config: config,
		}
	default:
		return nil
	}
}
