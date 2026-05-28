package logbroker

import (
	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debezium_parameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/middlewares/synchronizer/bufferer"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
	ydb_topics_sink "github.com/transferia/transferia/pkg/providers/ydb/topics/sink"
	"github.com/transferia/transferia/pkg/util/queues/coherence_check"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
)

const defaultLogbrokerDatabase = "/Root"

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
	Credentials       provider_ydb.TokenCredentials
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

type CompressionCodec ydb_topics_sink.CompressionCodec

const (
	CompressionCodecUnspecified = CompressionCodec(ydb_topics_sink.CompressionCodecUnspecified)
	CompressionCodecRaw         = CompressionCodec(ydb_topics_sink.CompressionCodecRaw)
	CompressionCodecGzip        = CompressionCodec(ydb_topics_sink.CompressionCodecGzip)
	CompressionCodecZstd        = CompressionCodec(ydb_topics_sink.CompressionCodecZstd)
)

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
		d.CompressionCodec = CompressionCodec(CompressionCodecGzip)
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
			Enabled:        false,
			Interval:       0,
			MaxChangeItems: 0,
			MaxMessageSize: 0,
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
	return debezium_parameters.GetYSRNamespaceID(d.FormatSettings.Settings)
}

func (d *LbDestination) Compatible(src model.Source, transferType abstract.TransferType) error {
	return coherence_check.SourceCompatible(src, transferType, d.FormatSettings.Name)
}

func (d *LbDestination) Serializer() (model.SerializationFormat, bool) {
	formatSettings := d.FormatSettings
	formatSettings.Settings = debezium_parameters.EnrichedWithDefaults(formatSettings.Settings)
	return formatSettings, d.SaveTxOrder
}

func (d *LbDestination) BuffererConfig() *bufferer.BuffererConfig {
	return &bufferer.BuffererConfig{
		TriggingCount:    d.FormatSettings.BatchingSettings.MaxChangeItems,
		TriggingSize:     uint64(d.FormatSettings.BatchingSettings.MaxMessageSize),
		TriggingInterval: d.FormatSettings.BatchingSettings.Interval,
	}
}

// TODO: Remove batching settings in this FillDependentFields in TM-9722.
func (d *LbDestination) FillDependentFields(transfer *model.Transfer) {
	if d.FormatSettings.BatchingSettings != nil && d.FormatSettings.BatchingSettings.Enabled {
		return
	}
	infered, err := coherence_check.InferFormatSettings(logger.Log, transfer.Src, d.FormatSettings)
	if err != nil {
		logger.Log.Warn("Unable to infer format settings to fill dependent fields", log.Error(err))
	}
	if infered.Name == model.SerializationFormatNative {
		// Forcely enable batching for Native.
		d.FormatSettings.BatchingSettings = &model.Batching{
			Enabled:        true,
			Interval:       0,
			MaxChangeItems: 1000,
			// Most destinations have limited size of one write (e.g. logbroker 120mb, yds 64mb).
			// Avoid using MaxMessageSize more than 64/2 (to avoid problem with too large messages), but also not too small.
			MaxMessageSize: 32 * humanize.MiByte,
		}
	}
}

func (d *LbDestination) db() string {
	if d.Database == "" {
		return defaultLogbrokerDatabase
	}
	return d.Database
}

func (d *LbDestination) TopicSinkConfig() *ydb_topics_sink.Config {
	return &ydb_topics_sink.Config{
		Connection: topiccommon.ConnectionConfig{
			Endpoint:    topiccommon.FormatEndpoint(d.Instance, d.Port),
			Database:    d.db(),
			Credentials: d.Credentials,
			TLSEnabled:  d.TLS == EnabledTLS,
			RootCAFiles: d.RootCAFiles,
		},

		Topic:            d.Topic,
		TopicPrefix:      d.TopicPrefix,
		CompressionCodec: ydb_topics_sink.CompressionCodec(d.CompressionCodec),
		FormatSettings:   d.FormatSettings,

		Shard: d.Shard,

		AddSystemTables: d.AddSystemTables,
		SaveTxOrder:     d.SaveTxOrder,
	}
}
