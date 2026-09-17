package queue_to_s3_sink

import (
	"strconv"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
)

type Partitioner interface {
	// Dir returns the directory the item belongs to. Items with different Dir must
	// never share a file: Rotator uses it to roll a new file as soon as an item stops
	// matching the directory of the currently open one.
	Dir(item *abstract.ChangeItem) (string, error)
	// ConstructKey returns the full object key of the file starting with the item.
	ConstructKey(item *abstract.ChangeItem) (string, error)
}

// Partitioner default <prefix>/<topic>/partition=<kafkaPartition>/<topic>+<kafkaPartition>+<startOffset>.<format>[.gz]
type DefaultPartitioner struct {
	config *BasePartitionerConfig
}

var _ Partitioner = (*DefaultPartitioner)(nil)

func (p *DefaultPartitioner) Dir(item *abstract.ChangeItem) (string, error) {
	if err := p.config.init(item); err != nil {
		return "", err
	}
	return p.config.dirPrefix() + "/partition=" + strconv.Itoa(p.config.Partition()), nil
}

func (p *DefaultPartitioner) ConstructKey(item *abstract.ChangeItem) (string, error) {
	dir, err := p.Dir(item)
	if err != nil {
		return "", err
	}
	return dir + "/" + p.config.fileName(item.QueueMessageMeta.Offset), nil
}

// Partitioner time based <prefix>/<topic>/<time bucket>/<topic>+<kafkaPartition>+<startOffset>.<format>[.gz]
type TimeBasedPartitioner struct {
	config        *BasePartitionerConfig
	timeCfg       *s3_v1_model.TimeBasedPartitionerConfig
	timeExtractor TimeExtractor
}

var _ Partitioner = (*TimeBasedPartitioner)(nil)

func (p *TimeBasedPartitioner) Dir(item *abstract.ChangeItem) (string, error) {
	if err := p.config.init(item); err != nil {
		return "", err
	}
	location, err := p.timeCfg.Location()
	if err != nil {
		return "", xerrors.Errorf("unable to load timezone %q: %w", p.timeCfg.Timezone, err)
	}
	pathFormat, err := p.timeCfg.PathFormat()
	if err != nil {
		return "", xerrors.Errorf("unable to resolve the time bucket layout: %w", err)
	}
	itemTime, err := p.timeExtractor.Extract(item)
	if err != nil {
		return "", xerrors.Errorf("unable to resolve the time bucket of the item: %w", err)
	}
	bucket := itemTime.In(location).Format(pathFormat)
	return p.config.dirPrefix() + "/" + bucket, nil
}

func (p *TimeBasedPartitioner) ConstructKey(item *abstract.ChangeItem) (string, error) {
	dir, err := p.Dir(item)
	if err != nil {
		return "", err
	}
	return dir + "/" + p.config.fileName(item.QueueMessageMeta.Offset), nil
}

// Base partitioner config is used by all partitioners regardless of its type
type BasePartitionerConfig struct {
	prefix      string
	topic       string
	partition   int
	serializer  s3_v1_model.SerializerConfig
	initialised bool
}

func (c *BasePartitionerConfig) Partition() int { return c.partition }
func (c *BasePartitionerConfig) Format() string {
	return strings.ToLower(string(c.serializer.FormatName()))
}

func (c *BasePartitionerConfig) IsGzip() bool {
	return c.serializer.FormatEncoding() == s3_v1_model.GzipEncoding
}

// init lazily completes the configuration from the first item it ever sees
func (c *BasePartitionerConfig) init(item *abstract.ChangeItem) error {
	if item == nil {
		return xerrors.Errorf("unable to extract data to construct next file name")
	}
	if c.initialised {
		return nil
	}

	c.topic = item.QueueMessageMeta.TopicName
	c.partition = item.QueueMessageMeta.PartitionNum

	if c.topic == "" {
		return xerrors.Errorf("failed to extract topic name or partition number from received message, message: %v", item)
	}

	c.initialised = true
	return nil
}

// dirPrefix returns <prefix>/<topic>, the part of the directory shared by all partitioners
func (c *BasePartitionerConfig) dirPrefix() string {
	if len(c.prefix) > 0 {
		return c.prefix + "/" + c.topic
	}
	return c.topic
}

// fileName returns <topic>+<kafkaPartition>+<startOffset>.<format>[.gz]
func (c *BasePartitionerConfig) fileName(offset uint64) string {
	var builder strings.Builder
	builder.Grow(c.fileNameLength(offset))

	builder.WriteString(c.topic)
	builder.WriteByte('+')
	builder.WriteString(strconv.Itoa(c.partition))
	builder.WriteByte('+')
	builder.WriteString(strconv.FormatUint(offset, 10))
	builder.WriteByte('.')
	builder.WriteString(c.Format())

	if c.IsGzip() {
		builder.WriteString(".gz")
	}
	return builder.String()
}

func (c *BasePartitionerConfig) fileNameLength(offset uint64) int {
	res := len(c.topic) + len("+") + len(strconv.Itoa(c.partition)) + len("+") + len(strconv.FormatUint(offset, 10))
	res += len(".") + len(c.Format())

	if c.IsGzip() {
		res += len(".gz")
	}
	return res
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

	switch t := cfg.GetPartitioner().(type) {
	case *s3_v1_model.DefaultPartitionerConfig:
		return &DefaultPartitioner{config: baseCfg}
	case *s3_v1_model.TimeBasedPartitionerConfig:
		return &TimeBasedPartitioner{
			config:        baseCfg,
			timeCfg:       t,
			timeExtractor: NewTimeExtractor(cfg.GetTimeExtractor()),
		}
	default:
		return nil
	}
}
