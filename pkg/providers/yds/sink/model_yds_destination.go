package sink

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debezium_parameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/middlewares/synchronizer/bufferer"
	provider_logbroker "github.com/transferia/transferia/pkg/providers/logbroker"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	ydb_topics_sink "github.com/transferia/transferia/pkg/providers/ydb/topics/sink"
	"github.com/transferia/transferia/pkg/providers/yds/yds_type"
	"github.com/transferia/transferia/pkg/util/queues/coherence_check"
	"go.uber.org/zap/zapcore"
)

type YDSDestination struct {
	DatabaseID       string                           `log:"true"`
	IsOnPremise      bool                             `log:"true"`
	LbDstConfig      provider_logbroker.LbDestination `log:"true"` // Connection
	SubNetworkID     string                           `log:"true"` // Connection
	SecurityGroupIDs []string                         `log:"true"` // Connection
	Underlay         bool                             `log:"true"` // Connection

	RootCAFiles      []string // Connection (always set in adapter)
	TLSEnalbed       bool     `log:"true"` // Connection (true if dp is external in adapter)
	TLSCACertificate string

	// Auth properties
	ServiceAccountID string             `log:"true"` // Connection (required)
	SAKeyContent     string             // Connection (always empty)
	TokenServiceURL  string             `log:"true"` // Connection (always empty)
	Token            model.SecretString // Connection (always empty)
	UserdataAuth     bool               `log:"true"` // Connection (always true for external dp in adapter)
}

func (d *YDSDestination) YSRNamespaceID() string {
	return debezium_parameters.GetYSRNamespaceID(d.LbDstConfig.FormatSettings.Settings)
}

func (d *YDSDestination) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(d, enc)
}

func (d *YDSDestination) IsUnderlayOnlyEndpoint() {}

func (d *YDSDestination) ServiceAccountIDs() []string {
	if d.ServiceAccountID != "" {
		return []string{d.ServiceAccountID}
	}
	return nil
}

var _ model.Destination = (*YDSDestination)(nil)
var _ sinkConfig = (*YDSDestination)(nil)

// EndpointParams

func (d *YDSDestination) MDBClusterID() string {
	if d.IsOnPremise {
		return ""
	}

	result := d.LbDstConfig.Database + "/" + d.LbDstConfig.Topic
	if result == "/" {
		return ""
	}
	return result
}

func (d *YDSDestination) GetProviderType() abstract.ProviderType {
	return yds_type.YDSProviderType
}

func (d *YDSDestination) Validate() error {
	if d.IsOnPremise && d.LbDstConfig.Instance == "" {
		return xerrors.New("instance parameter must be specified")
	}
	if d.LbDstConfig.TopicPrefix != "" && d.LbDstConfig.SaveTxOrder {
		return xerrors.Errorf("option 'SaveTxOrder'=true is incompatible with 'TopicPrefix'. Use either full topic name or turn off 'SaveTxOrder'.")
	}
	return nil
}

func (d *YDSDestination) Compatible(src model.Source, transferType abstract.TransferType) error {
	return coherence_check.SourceCompatible(src, transferType, d.LbDstConfig.FormatSettings.Name)
}

func (d *YDSDestination) WithDefaults() {
	if d.LbDstConfig.CompressionCodec == "" {
		d.LbDstConfig.CompressionCodec = provider_logbroker.CompressionCodecRaw
	}
	d.LbDstConfig.WithDefaults()
	d.LbDstConfig.Port = 2135
}

// Destination

func (d *YDSDestination) IsDestination() {}

func (d *YDSDestination) Transformer() map[string]string {
	return d.LbDstConfig.TransformerConfig
}

func (d *YDSDestination) CleanupMode() model.CleanupType {
	return d.LbDstConfig.Cleanup
}

// other

func (d *YDSDestination) Serializer() (model.SerializationFormat, bool) {
	formatSettings := d.LbDstConfig.FormatSettings
	formatSettings.Settings = debezium_parameters.EnrichedWithDefaults(formatSettings.Settings)
	return formatSettings, d.LbDstConfig.SaveTxOrder
}

func (d *YDSDestination) BuffererConfig() *bufferer.BuffererConfig {
	return &bufferer.BuffererConfig{
		TriggingCount:    d.LbDstConfig.FormatSettings.BatchingSettings.MaxChangeItems,
		TriggingSize:     uint64(d.LbDstConfig.FormatSettings.BatchingSettings.MaxMessageSize),
		TriggingInterval: d.LbDstConfig.FormatSettings.BatchingSettings.Interval,
	}
}

// TODO: Remove batching settings in LbDstConfig.FillDependentFields in TM-9722.
func (d *YDSDestination) FillDependentFields(transfer *model.Transfer) {
	d.LbDstConfig.FillDependentFields(transfer)
}

func (d *YDSDestination) prepareConfig() error {
	if d.TLSCACertificate != "" {
		d.LbDstConfig.TLS = provider_logbroker.EnabledTLS
		d.LbDstConfig.TLSCACertificate = d.TLSCACertificate
	} else if d.TLSEnalbed {
		d.LbDstConfig.TLS = provider_logbroker.EnabledTLS
		d.LbDstConfig.RootCAFiles = d.RootCAFiles
	}

	creds, err := provider_ydb.ResolveCredentials(
		d.UserdataAuth,
		string(d.Token),
		provider_ydb.JWTAuthParams{
			KeyContent:      d.SAKeyContent,
			TokenServiceURL: d.TokenServiceURL,
		},
		d.ServiceAccountID,
		nil,
		logger.Log,
	)
	if err != nil {
		return xerrors.Errorf("cannot create YDB credentials: %w", err)
	}
	d.LbDstConfig.Credentials = creds

	return nil
}

func (d *YDSDestination) topicSinkConfig() (*ydb_topics_sink.Config, error) {
	return d.LbDstConfig.TopicSinkConfig()
}
