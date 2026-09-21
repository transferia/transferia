package sink

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
	"github.com/transferia/transferia/pkg/providers/yds/yds_type"
	"github.com/transferia/transferia/pkg/util/queues/coherence_check"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
)

type YDBTopicDestination struct {
	Endpoint string `log:"true"`

	// DatabaseID is the Managed YDB database ID; when set, Database is resolved from it.
	DatabaseID string `log:"true"`
	Database   string `log:"true"`

	Token       model.SecretString
	Credentials provider_ydb.TokenCredentials
	TLS         model.TLSMode `log:"true"`
	RootCAFiles []string

	Topic          string                    `log:"true"`
	TopicPrefix    string                    `log:"true"`
	FormatSettings model.SerializationFormat `log:"true"`
	Shard          string                    `log:"true"`

	AddSystemTables bool `log:"true"`
	SaveTxOrder     bool `log:"true"`
}

var _ model.Destination = (*YDBTopicDestination)(nil)
var _ sinkConfig = (*YDBTopicDestination)(nil)

func (d *YDBTopicDestination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
}

func (d *YDBTopicDestination) WithDefaults() {
	if d.TLS == "" {
		d.TLS = model.DefaultTLS
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

func (YDBTopicDestination) IsDestination() {}

func (d *YDBTopicDestination) GetProviderType() abstract.ProviderType {
	return yds_type.YDBTopicProviderType
}

func (d *YDBTopicDestination) Validate() error {
	if d.Topic == "" && d.TopicPrefix == "" {
		return xerrors.New("one of 'Topic' or 'TopicPrefix' must be set")
	}
	if d.Topic != "" && d.TopicPrefix != "" {
		return xerrors.New("'Topic' and 'TopicPrefix' are mutually exclusive, set only one of them")
	}
	if d.TopicPrefix != "" && d.SaveTxOrder {
		return xerrors.Errorf("option 'SaveTxOrder'=true is incompatible with 'TopicPrefix'. Use either full topic name or turn off 'SaveTxOrder'.")
	}
	return nil
}

func (d *YDBTopicDestination) CleanupMode() model.CleanupType {
	return model.DisabledCleanup
}

func (d *YDBTopicDestination) YSRNamespaceID() string {
	return debezium_parameters.GetYSRNamespaceID(d.FormatSettings.Settings)
}

func (d *YDBTopicDestination) Compatible(src model.Source, transferType abstract.TransferType) error {
	return coherence_check.SourceCompatible(src, transferType, d.FormatSettings.Name)
}

func (d *YDBTopicDestination) Serializer() (model.SerializationFormat, bool) {
	formatSettings := d.FormatSettings
	formatSettings.Settings = debezium_parameters.EnrichedWithDefaults(formatSettings.Settings)
	return formatSettings, d.SaveTxOrder
}

func (d *YDBTopicDestination) BuffererConfig() *bufferer.BuffererConfig {
	return &bufferer.BuffererConfig{
		TriggingCount:    d.FormatSettings.BatchingSettings.MaxChangeItems,
		TriggingSize:     uint64(d.FormatSettings.BatchingSettings.MaxMessageSize),
		TriggingInterval: d.FormatSettings.BatchingSettings.Interval,
	}
}

func (d *YDBTopicDestination) FillDependentFields(transfer *model.Transfer) {
	if d.FormatSettings.BatchingSettings != nil && d.FormatSettings.BatchingSettings.Enabled {
		return
	}
	inferred, err := coherence_check.InferFormatSettings(logger.Log, transfer.Src, d.FormatSettings)
	if err != nil {
		logger.Log.Warn("Unable to infer format settings to fill dependent fields", log.Error(err))
	}
	if inferred.Name == model.SerializationFormatNative {
		d.FormatSettings.BatchingSettings = &model.Batching{
			Enabled:        true,
			Interval:       0,
			MaxChangeItems: 1000,
			MaxMessageSize: 32 * humanize.MiByte,
		}
	}
}

func (d *YDBTopicDestination) prepareConfig() error {
	if d.Credentials != nil {
		return nil
	}

	var err error
	d.Credentials, err = provider_ydb.ResolveCredentials(
		false,
		string(d.Token),
		provider_ydb.JWTAuthParams{
			KeyContent:      "",
			TokenServiceURL: "",
		},
		"",
		nil,
		logger.Log,
	)
	if err != nil {
		return xerrors.Errorf("cannot create YDB credentials: %w", err)
	}

	return nil
}

func (d *YDBTopicDestination) topicSinkConfig() (*ydb_topics_sink.Config, error) {
	return &ydb_topics_sink.Config{
		Connection: topiccommon.ConnectionConfig{
			Endpoint:         d.Endpoint,
			Database:         d.Database,
			Credentials:      d.Credentials,
			TLSEnabled:       d.TLS == model.EnabledTLS,
			RootCAFiles:      d.RootCAFiles,
			TLSCACertificate: "",
		},
		Topic:            d.Topic,
		TopicPrefix:      d.TopicPrefix,
		CompressionCodec: ydb_topics_sink.CompressionCodecGzip,
		FormatSettings:   d.FormatSettings,
		Shard:            d.Shard,
		AddSystemTables:  d.AddSystemTables,
		SaveTxOrder:      d.SaveTxOrder,
	}, nil
}
