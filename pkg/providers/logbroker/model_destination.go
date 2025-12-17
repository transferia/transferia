package logbroker

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/middlewares/async/bufferer"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"github.com/transferia/transferia/pkg/util/queues/coherence_check"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"go.uber.org/zap/zapcore"
)

type LbDestination struct {
	Instance string `log:"true"`
	Database string `log:"true"`

	Token             string
	Shard             string            `log:"true"`
	TLS               TLSMode           `log:"true"`
	TransformerConfig map[string]string `log:"true"`
	Cleanup           model.CleanupType `log:"true"`
	MaxChunkSize      uint              `log:"true"` // Deprecated, can be deleted, but I'm scared by the GOB
	WriteTimeoutSec   int               `log:"true"` // Deprecated
	Credentials       ydb.TokenCredentials
	Port              int              `log:"true"`
	CompressionCodec  CompressionCodec `log:"true"`

	Topic       string `log:"true"` // full-name version
	TopicPrefix string `log:"true"`

	AddSystemTables bool `log:"true"` // private options - to not skip consumer_keeper & other system tables
	SaveTxOrder     bool `log:"true"`

	// for now, 'FormatSettings' is private option - it's WithDefaults(): SerializationFormatAuto - 'Mirror' for queues, 'Debezium' for the rest
	FormatSettings model.SerializationFormat `log:"true"`

	RootCAFiles []string
}

var _ model.Destination = (*LbDestination)(nil)

type TLSMode = model.TLSMode

const (
	DefaultTLS  = model.DefaultTLS
	EnabledTLS  = model.EnabledTLS
	DisabledTLS = model.DisabledTLS
)

type CompressionCodec string

const (
	CompressionCodecUnspecified CompressionCodec = ""
	CompressionCodecRaw         CompressionCodec = "raw"
	CompressionCodecGzip        CompressionCodec = "gzip"
	CompressionCodecZstd        CompressionCodec = "zstd"
)

func (e CompressionCodec) ToTopicTypesCodec() topictypes.Codec {
	switch e {
	case CompressionCodecGzip:
		return topictypes.CodecGzip
	case CompressionCodecZstd:
		return topictypes.CodecZstd
	default:
		return topictypes.CodecRaw
	}
}

func (d *LbDestination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
}

func (d *LbDestination) IsEmpty() bool {
	// Case for function 'getEndpointsCreateFormDefaultsDynamic'
	// In this case 'KafkaDestination' model is initialized by default values, and we can set defaults for one-of
	return d.Topic == "" && d.TopicPrefix == ""
}

func (d *LbDestination) WithDefaults() {
	if d.CompressionCodec == "" {
		d.CompressionCodec = CompressionCodecGzip
	}
	if d.Cleanup == "" {
		d.Cleanup = model.DisabledCleanup
	}
	if d.TLS == "" {
		d.TLS = DefaultTLS
	}
	if d.FormatSettings.Name == "" {
		d.FormatSettings.Name = model.SerializationFormatAuto
	}
	if d.FormatSettings.Settings == nil {
		d.FormatSettings.Settings = make(map[string]string)
	}
	if d.FormatSettings.BatchingSettings == nil {
		d.FormatSettings.BatchingSettings = &model.Batching{
			Enabled:        true,
			Interval:       0,
			MaxChangeItems: 1000,
			// there is a limit on the total size of messages in one write (logbroker 120mb, yds 64mb), so
			// the value chosen here is not more than 64/2 (to avoid problem with too large messages), but also not too small
			MaxMessageSize: 32 * 1024 * 1024,
		}
	}
}

func (d *LbDestination) CleanupMode() model.CleanupType {
	return d.Cleanup
}

func (d *LbDestination) Transformer() map[string]string {
	return d.TransformerConfig
}

func (LbDestination) IsDestination() {}

func (d *LbDestination) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (d *LbDestination) Validate() error {
	if d.TopicPrefix != "" && d.SaveTxOrder {
		return xerrors.Errorf("option 'SaveTxOrder'=true is incompatible with 'TopicPrefix'. Use either full topic name or turn off 'SaveTxOrder'.")
	}
	return nil
}

func (d *LbDestination) YSRNamespaceID() string {
	return debeziumparameters.GetYSRNamespaceID(d.FormatSettings.Settings)
}

func (d *LbDestination) Compatible(src model.Source, transferType abstract.TransferType) error {
	return coherence_check.SourceCompatible(src, transferType, d.FormatSettings.Name)
}

func (d *LbDestination) IsTransitional() {}

func (d *LbDestination) TransitionalWith(left model.TransitionalEndpoint) bool {
	if src, ok := left.(*LbSource); ok {
		return d.Instance == src.Instance && d.Topic == src.Topic
	}
	return false
}

func (d *LbDestination) Serializer() (model.SerializationFormat, bool) {
	formatSettings := d.FormatSettings
	formatSettings.Settings = debeziumparameters.EnrichedWithDefaults(formatSettings.Settings)
	return formatSettings, d.SaveTxOrder
}

func (d *LbDestination) BuffererConfig() *bufferer.BuffererConfig {
	return &bufferer.BuffererConfig{
		TriggingCount:    d.FormatSettings.BatchingSettings.MaxChangeItems,
		TriggingSize:     uint64(d.FormatSettings.BatchingSettings.MaxMessageSize),
		TriggingInterval: d.FormatSettings.BatchingSettings.Interval,
	}
}

func (d *LbDestination) DB() string {
	if d.Database == "" {
		return "/Root"
	}
	return d.Database
}

func (d *LbDestination) InstanceWithPort() string {
	res := d.Instance
	if instanceContainsPort(res) {
		return res
	}

	port := 2135
	if d.Port != 0 {
		port = d.Port
	}

	return fmt.Sprintf("%s:%d", res, port)
}

func instanceContainsPort(instance string) bool {
	parts := strings.Split(instance, ":")
	if len(parts) < 2 {
		return false
	}

	intendedPort := parts[len(parts)-1]
	for _, c := range intendedPort {
		if !unicode.IsDigit(c) {
			return false
		}
	}

	return true
}
