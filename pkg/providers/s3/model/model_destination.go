package s3_model

import (
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares/synchronizer/bufferer"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
	"go.uber.org/zap/zapcore"
)

func init() {
	gobwrapper.RegisterName("*server.S3Destination", new(S3Destination))
	model.RegisterDestination(ProviderType, func() model.LoggableDestination {
		return new(S3Destination)
	})
	abstract.RegisterProviderName(ProviderType, "ObjectStorage")
}

const (
	ProviderType = abstract.ProviderType("s3")
)

type Encoding string

const (
	NoEncoding   = Encoding("UNCOMPRESSED")
	GzipEncoding = Encoding("GZIP")
	ZlibEncoding = Encoding("ZLIB")
)

type SerializerSettings struct {
	Parquet *ParquetSerializerSettings `log:"true"`
}

func (s *SerializerSettings) GetParquet() *ParquetSerializerSettings {
	if s == nil {
		return nil
	}
	return s.Parquet
}

type ParquetSerializerSettings struct {
	CompressionCodec string `log:"true"`
	RowGroupMaxRows  int64  `log:"true"`
	RowGroupMaxBytes int64  `log:"true"`
}

type PartitionerType string

const (
	DefaultPartitionerType PartitionerType = "DefaultPartitioner"
)

type Rotator interface {
	ShouldRotate(item *abstract.ChangeItem) bool
	UpdateState(item *abstract.ChangeItem) error
}

type S3Destination struct {
	OutputFormat     model.ParsingFormat `log:"true"`
	OutputEncoding   Encoding            `log:"true"`
	BufferSize       model.BytesSize     `log:"true"`
	BufferInterval   time.Duration       `log:"true"`
	Endpoint         string              `log:"true"`
	Region           string              `log:"true"`
	AccessKey        string
	S3ForcePathStyle bool `log:"true"`
	Secret           string
	ServiceAccountID string `log:"true"`
	Layout           string `log:"true"`
	LayoutTZ         string `log:"true"`
	LayoutColumn     string `log:"true"`
	Bucket           string `log:"true"`
	UseSSL           bool   `log:"true"`
	VerifySSL        bool   `log:"true"`
	PartSize         int64  `log:"true"`
	Concurrency      int64  `log:"true"`
	AnyAsString      bool   `log:"true"`
	MaxItemsPerFile  int    `log:"true"`
	MaxBytesPerFile  int    `log:"true"`
	SerializerSet    bool
	Cleanup          model.CleanupType `log:"true"`

	SerializerSettings SerializerSettings `log:"true"`

	// Replication from queue specific fields
	Rotator     Rotator
	Partitioner PartitionerType
}

var _ model.Destination = (*S3Destination)(nil)

func (d *S3Destination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
}

func (d *S3Destination) WithDefaults() {
	if d.Layout == "" {
		d.Layout = "2006/01/02"
	}
	if d.BufferInterval == 0 {
		d.BufferInterval = time.Second * 30
	}
	if d.BufferSize == 0 {
		d.BufferSize = model.BytesSize(clickhouse_model.BufferTriggingSizeDefault)
	}
	if d.Concurrency == 0 {
		d.Concurrency = 4
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
	if d.ServiceAccountID != "" {
		return []string{d.ServiceAccountID}
	}
	return nil
}

func (d *S3Destination) ConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		AccessKey:        d.AccessKey,
		S3ForcePathStyle: d.S3ForcePathStyle,
		SecretKey:        model.SecretString(d.Secret),
		Endpoint:         d.Endpoint,
		UseSSL:           d.UseSSL,
		VerifySSL:        d.VerifySSL,
		Region:           d.Region,
		ServiceAccountID: d.ServiceAccountID,
	}
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

func (d *S3Destination) compatible(src model.Source) bool {
	parseable, ok := src.(model.Parseable)
	if d.OutputFormat == model.ParsingFormatRaw {
		if ok {
			return parseable.Parser() == nil
		}
		return false
	} else {
		if ok {
			return parseable.Parser() != nil
		}
		return true
	}
}

func (d *S3Destination) Compatible(src model.Source, _ abstract.TransferType) error {
	if d.compatible(src) {
		return nil
	}
	return xerrors.Errorf("object storage %s format not compatible", d.OutputFormat)
}
