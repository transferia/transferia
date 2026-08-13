package source

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
	topicsource "github.com/transferia/transferia/pkg/providers/ydb/topics/source"
	"github.com/transferia/transferia/pkg/providers/yds/yds_type"
	"go.uber.org/zap/zapcore"
)

type YDBTopicSource struct {
	Endpoint string `log:"true"`
	Database string `log:"true"`

	Credentials provider_ydb.TokenCredentials
	TLS         model.TLSMode `log:"true"`
	RootCAFiles []string

	Topics   []string `log:"true"`
	Consumer string   `log:"true"`

	ParserConfig map[string]interface{} `log:"true"`

	IsYDBTopicSink        bool `log:"true"`
	AllowTTLRewind        bool `log:"true"`
	ParseQueueParallelism int  `log:"true"`
}

var _ model.Source = (*YDBTopicSource)(nil)
var _ sourceConfig = (*YDBTopicSource)(nil)

func (s *YDBTopicSource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(s, enc)
}

func (s *YDBTopicSource) WithDefaults() {
	if s.TLS == "" {
		s.TLS = model.DefaultTLS
	}
	if s.ParseQueueParallelism == 0 {
		s.ParseQueueParallelism = 10
	}
}

func (YDBTopicSource) IsSource() {}

func (s *YDBTopicSource) GetProviderType() abstract.ProviderType {
	return yds_type.YDBTopicProviderType
}

func (s *YDBTopicSource) Validate() error {
	if s.ParserConfig != nil {
		parserConfigStruct, err := parsers.ParserConfigMapToStruct(s.ParserConfig)
		if err != nil {
			return xerrors.Errorf("unable to create parser config: %w", err)
		}
		return parserConfigStruct.Validate()
	}
	return nil
}

func (s *YDBTopicSource) topicSourceConfig(_ string) *topicsource.Config {
	return &topicsource.Config{
		Connection: topiccommon.ConnectionConfig{
			Endpoint:         s.Endpoint,
			Database:         s.Database,
			Credentials:      s.Credentials,
			TLSEnabled:       s.TLS == model.EnabledTLS,
			RootCAFiles:      s.RootCAFiles,
			TLSCACertificate: "",
		},
		Topics:      s.Topics,
		Consumer:    s.Consumer,
		ReaderOpts:  topicsource.NewDefaultReaderOptions(),
		Transformer: nil,

		IsYDBTopicSink:             s.IsYDBTopicSink,
		AllowTTLRewind:             s.AllowTTLRewind,
		ParseQueueParallelism:      s.ParseQueueParallelism,
		UseFullTopicNameForParsing: false,
	}
}
