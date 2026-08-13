package source

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	topicsource "github.com/transferia/transferia/pkg/providers/ydb/topics/source"
	"github.com/transferia/transferia/pkg/providers/yds/yds_type"
	"go.uber.org/zap/zapcore"
)

type YDSSource struct {
	DatabaseID       string                      `log:"true"`
	IsOnPremise      bool                        `log:"true"`
	Endpoint         string                      `log:"true"` // Connection (if empty set in adapter)
	Database         string                      `log:"true"` // Connection
	Stream           string                      `log:"true"` // Connection
	Consumer         string                      `log:"true"`
	S3BackupBucket   string                      `model:"ObjectStorageBackupBucket" log:"true"` // Is never used
	Port             int                         `log:"true"`                                   // Connection but is never set
	BackupMode       model.BackupMode            `log:"true"`                                   // Is never used
	Transformer      *model.DataTransformOptions `log:"true"`
	SubNetworkID     string                      `log:"true"` // Connection
	SecurityGroupIDs []string                    `log:"true"` // Connection
	SupportedCodecs  []YdsCompressionCodec       `log:"true"` // TODO: Replace with pq codecs?
	AllowTTLRewind   bool                        `log:"true"`

	IsLbSink bool `log:"true"` // it's like IsHomo

	TLSEnalbed       bool     `log:"true"` // Connection (is always true for external dp in adapter)
	RootCAFiles      []string // Connection (is always set in adapter)
	TLSCACertificate string

	ParserConfig map[string]interface{} `log:"true"`
	Underlay     bool                   `log:"true"` // Connection

	// Auth properties
	Credentials           provider_ydb.TokenCredentials // Connection (set in runtime only)
	ServiceAccountID      string                        `model:"ServiceAccountId" log:"true"` // Connection
	SAKeyContent          string                        // Connection but is never set
	TokenServiceURL       string                        `log:"true"` // Connection but is never set
	Token                 model.SecretString            // Connection (is only set when dataplane is internal in adapter)
	UserdataAuth          bool                          `log:"true"` // Connection (is always true for external dp in adapter)
	ParseQueueParallelism int                           `log:"true"`
}

func (s *YDSSource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(s, enc)
}

func (s *YDSSource) IsUnderlayOnlyEndpoint() {}

func (s *YDSSource) ServiceAccountIDs() []string {
	var saIDs []string
	if s.ServiceAccountID != "" {
		saIDs = append(saIDs, s.ServiceAccountID)
	}
	if s.Transformer != nil && s.Transformer.ServiceAccountID != "" {
		saIDs = append(saIDs, s.Transformer.ServiceAccountID)
	}
	return saIDs
}

type YdsCompressionCodec int

const (
	YdsCompressionCodecRaw  = YdsCompressionCodec(1)
	YdsCompressionCodecGzip = YdsCompressionCodec(2)
	YdsCompressionCodecZstd = YdsCompressionCodec(4)
)

var _ model.Source = (*YDSSource)(nil)
var _ model.QueueToS3Source = (*YDSSource)(nil)

func (s *YDSSource) MDBClusterID() string {
	if s.IsOnPremise {
		return ""
	}
	return s.Database + "/" + s.Stream
}

func (s *YDSSource) Dedicated(publicEndpoint string) bool {
	return s.Endpoint != "" && s.Endpoint != publicEndpoint
}

func (s *YDSSource) GetSupportedCodecs() []YdsCompressionCodec {
	if len(s.SupportedCodecs) == 0 {
		return []YdsCompressionCodec{YdsCompressionCodecRaw}
	}
	return s.SupportedCodecs
}

func (s *YDSSource) WithDefaults() {
	if s.BackupMode == "" {
		s.BackupMode = model.S3BackupModeNoBackup
	}
	if s.Port == 0 {
		s.Port = 2135
	}
	if s.Transformer != nil && s.Transformer.CloudFunction == "" {
		s.Transformer = nil
	}
}

func (s *YDSSource) IsSource() {}

func (s *YDSSource) GetProviderType() abstract.ProviderType {
	return yds_type.YDSProviderType
}

func (s *YDSSource) Validate() error {
	if s.IsOnPremise && s.Endpoint == "" {
		return xerrors.New("instance parameter must be specified")
	}
	if s.ParserConfig != nil {
		parserConfigStruct, err := parsers.ParserConfigMapToStruct(s.ParserConfig)
		if err != nil {
			return xerrors.Errorf("unable to create new parser config, err: %w", err)
		}
		return parserConfigStruct.Validate()
	}
	return nil
}

func (s *YDSSource) IsAppendOnly() bool {
	if s.ParserConfig == nil {
		return false
	} else {
		parserConfigStruct, _ := parsers.ParserConfigMapToStruct(s.ParserConfig)
		if parserConfigStruct == nil {
			return false
		}
		return parserConfigStruct.IsAppendOnly()
	}
}

func (s *YDSSource) YSRNamespaceID() string {
	if s.ParserConfig == nil {
		return ""
	} else {
		parserConfigStruct, _ := parsers.ParserConfigMapToStruct(s.ParserConfig)
		if parserConfigStruct == nil {
			return ""
		}
		if parserConfigStructYSRable, ok := parserConfigStruct.(parsers.YSRable); ok {
			return parserConfigStructYSRable.YSRNamespaceID()
		}
		return ""
	}
}

func (s *YDSSource) IsDefaultMirror() bool {
	return s.ParserConfig == nil
}

func (s *YDSSource) Parser() map[string]interface{} {
	return s.ParserConfig
}

func (s *YDSSource) IsQueueToS3Source() {}

func (s *YDSSource) topicSourceConfig(defaultConsumer string) *topicsource.Config {
	return buildTopicSourceConfig(defaultConsumer, s)
}
