package s3_model

import (
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares/synchronizer/bufferer"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.uber.org/zap/zapcore"
)

func init() {
	gobwrapper.RegisterName("*server.S3V1Destination", new(S3Destination))
	model.RegisterDestination(ProviderType, func() model.LoggableDestination {
		return new(S3Destination)
	})
	abstract.RegisterProviderName(ProviderType, "ObjectStorage")
}

const (
	ProviderType = abstract.ProviderType("object_storage_v1")
)

type S3Destination struct {
	// Common
	PartSize int64             `log:"true"` // Currently is hardcoded
	Bucket   string            `log:"true"`
	Cleanup  model.CleanupType `log:"true"`

	SerializerSet bool // for api consistency

	// Common serialiser settings
	SerializerType model.ParsingFormat `log:"true"`
	Serializer     SerializerUnion     `log:"true"`

	// New common
	Prefix string `log:"true"` // prefix of the path

	// Impossible to set, hardcoded like in postgres
	BufferSize     model.BytesSize `log:"true"`
	BufferInterval time.Duration   `log:"true"`

	// Common connection
	Connection s3_model.ConnectionConfig

	/*
		Only differences between replication and snapshot configuration is way to establish rotation config
		+ presence of partitioning in replication while hardcoded logic in snapshot

		Maybe we should unify these two -> no need to divide forms
	*/

	// Snapshot
	MaxItemsPerFile int `log:"true"`
	MaxBytesPerFile int `log:"true"`

	// Replication
	RotatorType       RotatorType      `log:"true"`
	RotatorConfig     RotatorUnion     `log:"true"`
	PartitionerType   PartitionerType  `log:"true"`
	PartitionerConfig PartitionerUnion `log:"true"`
}

var (
	_ model.Destination          = (*S3Destination)(nil)
	_ model.QueueToS3Destination = (*S3Destination)(nil)
)

func (d *S3Destination) IsQueueToS3Destination() {
}

func (d *S3Destination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
}

func (d *S3Destination) WithDefaults() {
	if d.BufferInterval == 0 {
		d.BufferInterval = time.Second * 30
	}
	if d.BufferSize == 0 {
		d.BufferSize = model.BytesSize(clickhouse_model.BufferTriggingSizeDefault)
	}
	if d.RotatorType == "" {
		d.RotatorConfig.Default = &DefaultRotatorConfig{Interval: 3600 * time.Second}
	}
	if d.PartitionerType == "" {
		d.PartitionerConfig.Default = &DefaultPartitionerConfig{}
	}
	if d.Cleanup == "" {
		d.Cleanup = model.DisabledCleanup
	}
}

func (d *S3Destination) BuffererConfig() *bufferer.BuffererConfig {
	return &bufferer.BuffererConfig{
		TriggingCount:    0,
		TriggingSize:     uint64(d.BufferSize),
		TriggingInterval: d.BufferInterval,
	}
}

func (d *S3Destination) ServiceAccountIDs() []string {
	if d.Connection.ServiceAccountID != "" {
		return []string{d.Connection.ServiceAccountID}
	}
	return nil
}

func (d *S3Destination) Transformer() map[string]string {
	return map[string]string{}
}

func (d *S3Destination) CleanupMode() model.CleanupType {
	return d.Cleanup
}

func (d *S3Destination) IsDestination() {
}

func (d *S3Destination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *S3Destination) Validate() error {
	return nil
}

// For Kafka and Logbroker
func (d *S3Destination) compatible(src model.Source) bool {
	if parseable, ok := src.(model.Parseable); ok {
		return parseable.Parser() != nil
	}
	return true
}

func (d *S3Destination) Compatible(src model.Source, _ abstract.TransferType) error {
	if d.compatible(src) {
		return nil
	}
	return xerrors.Errorf("object storage %s format not compatible", d.GetSerializer().FormatName())
}

func (d *S3Destination) GetSerializer() SerializerConfig {
	switch d.SerializerType {
	case model.ParsingFormatCSV:
		return d.Serializer.CSV
	case model.ParsingFormatJSON:
		return d.Serializer.Json
	case model.ParsingFormatPARQUET:
		return d.Serializer.Parquet
	}
	return nil
}

func (d *S3Destination) GetRotator() RotatorConfig {
	switch d.RotatorType {
	case DefaultRotator:
		return d.RotatorConfig.Default
	default:
		return d.RotatorConfig.Default
	}
}

func (d *S3Destination) GetPartitioner() PartitionerConfig {
	switch d.PartitionerType {
	case DefaultPartitioner:
		return d.PartitionerConfig.Default
	default:
		return d.PartitionerConfig.Default
	}
}
