package s3

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares/async/bufferer"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
)

func init() {
	gobwrapper.RegisterName("*server.S3Destination", new(S3Destination))
	dp_model.RegisterDestination(ProviderType, func() dp_model.Destination {
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
)

type S3Destination struct {
	OutputFormat     dp_model.ParsingFormat
	OutputEncoding   Encoding
	BufferSize       dp_model.BytesSize
	BufferInterval   time.Duration
	Endpoint         string
	Region           string
	AccessKey        string
	S3ForcePathStyle bool
	Secret           string
	ServiceAccountID string
	Layout           string
	LayoutTZ         string
	LayoutColumn     string
	Bucket           string
	UseSSL           bool
	VerifySSL        bool
	PartSize         int64
	Concurrency      int64
	AnyAsString      bool
}

var _ dp_model.Destination = (*S3Destination)(nil)

func (d *S3Destination) WithDefaults() {
	if d.Layout == "" {
		d.Layout = "2006/01/02"
	}
	if d.BufferInterval == 0 {
		d.BufferInterval = time.Second * 30
	}
	if d.BufferSize == 0 {
		d.BufferSize = dp_model.BytesSize(model.BufferTriggingSizeDefault)
	}
	if d.Concurrency == 0 {
		d.Concurrency = 4
	}
}

func (d *S3Destination) BuffererConfig() *bufferer.BuffererConfig {
	return &bufferer.BuffererConfig{
		TriggingCount:    0,
		TriggingSize:     uint64(d.BufferSize),
		TriggingInterval: d.BufferInterval,
	}
}

func (d *S3Destination) ConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		AccessKey:        d.AccessKey,
		S3ForcePathStyle: d.S3ForcePathStyle,
		SecretKey:        dp_model.SecretString(d.Secret),
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

func (d *S3Destination) CleanupMode() dp_model.CleanupType {
	return dp_model.DisabledCleanup
}

func (S3Destination) IsDestination() {
}

func (d *S3Destination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *S3Destination) Validate() error {
	return nil
}

func (d *S3Destination) compatible(src dp_model.Source) bool {
	parseable, ok := src.(dp_model.Parseable)
	if d.OutputFormat == dp_model.ParsingFormatRaw {
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

func (d *S3Destination) Compatible(src dp_model.Source, _ abstract.TransferType) error {
	if d.compatible(src) {
		return nil
	}
	return xerrors.Errorf("object storage %s format not compatible", d.OutputFormat)
}
