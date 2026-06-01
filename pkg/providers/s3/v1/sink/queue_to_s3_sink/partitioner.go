package queue_to_s3_sink

import (
	"strconv"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
)

type Partitioner interface {
	ConstructKey(item *abstract.ChangeItem) (string, error)
}

// Partitioner default <prefix>/<topic>/partition=<kafkaPartition>/<topic>+<kafkaPartition>+<startOffset>.<format>[.gz]
type DefaultPartitioner struct {
	config *BasePartitionerConfig
}

var _ Partitioner = (*DefaultPartitioner)(nil)

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

// Base partitioner config is used by all partitioners regardless of its type
type BasePartitionerConfig struct {
	prefix      string
	topic       string
	partition   int
	serializer  s3_v1_model.SerializerConfig
	initialised bool
}

func (c *BasePartitionerConfig) Prefix() string { return c.prefix }
func (c *BasePartitionerConfig) Topic() string  { return c.topic }
func (c *BasePartitionerConfig) Partition() int { return c.partition }
func (c *BasePartitionerConfig) Format() string {
	return strings.ToLower(string(c.serializer.FormatName()))
}

func (c *BasePartitionerConfig) IsGzip() bool {
	return c.serializer.FormatEncoding() == s3_v1_model.GzipEncoding
}
func (c *BasePartitionerConfig) Initialised() bool { return c.initialised }

func (c *BasePartitionerConfig) FinishInit(item *abstract.ChangeItem) error {
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

func NewBasePartitionerConfig(cfg *s3_v1_model.S3Destination) *BasePartitionerConfig {
	return &BasePartitionerConfig{
		prefix:      cfg.Prefix,
		topic:       "", // Extract from first message
		partition:   -1, // Extract from first message
		serializer:  cfg.GetSerializer(),
		initialised: false,
	}
}

func NewPartitioner(cfg *s3_v1_model.S3Destination) Partitioner {
	baseCfg := NewBasePartitionerConfig(cfg)

	switch cfg.GetPartitioner().(type) {
	case *s3_v1_model.DefaultPartitionerConfig:
		return &DefaultPartitioner{config: baseCfg}
	default:
		return nil
	}
}
