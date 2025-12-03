package logbroker

import (
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/maps"
)

type LfSource struct {
	Instance             LogbrokerInstance `log:"true"`
	Cluster              LogbrokerCluster  `log:"true"`
	Database             string            `log:"true"`
	Token                string
	Consumer             string          `log:"true"`
	MaxReadSize          model.BytesSize `log:"true"`
	MaxMemory            model.BytesSize `log:"true"`
	MaxTimeLag           time.Duration   `log:"true"`
	Topics               []string        `log:"true"`
	MaxIdleTime          time.Duration   `log:"true"`
	MaxReadMessagesCount uint32          `log:"true"`
	OnlyLocal            bool            `log:"true"`
	LfParser             bool            `log:"true"`
	Credentials          ydb.TokenCredentials
	Port                 int  `log:"true"`
	AllowTTLRewind       bool `log:"true"`

	IsLbSink bool `log:"true"` // it's like IsHomo

	ParserConfig map[string]interface{} `log:"true"`

	TLS                   TLSMode `log:"true"`
	RootCAFiles           []string
	ParseQueueParallelism int `log:"true"`

	UsePqv1 bool `log:"true"`
}

var _ model.Source = (*LfSource)(nil)

type LogbrokerInstance string
type LogbrokerCluster string

func (s *LfSource) IsLbMirror() bool {
	if len(s.ParserConfig) == 0 {
		return false
	} else {
		return maps.Keys(s.ParserConfig)[0] == "blank.lb"
	}
}

func (s *LfSource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(s, enc)
}

func (s *LfSource) WithDefaults() {
	if s.MaxReadSize == 0 {
		// By default, 1 mb, we will extract it in 10-15 mbs.
		s.MaxReadSize = 1 * 1024 * 1024
	}

	if s.MaxMemory == 0 {
		// large then max memory to be able to hold at least 2 message batch in memory
		s.MaxMemory = s.MaxReadSize * 50
	}
}

func (LfSource) IsSource() {}

func (s *LfSource) GetProviderType() abstract.ProviderType {
	return ProviderWithParserType
}

func (s *LfSource) Validate() error {
	parserConfigStruct, err := parsers.ParserConfigMapToStruct(s.ParserConfig)
	if err != nil {
		return xerrors.Errorf("unable to make parser from config, err: %w", err)
	}
	return parserConfigStruct.Validate()
}

func (s *LfSource) IsAppendOnly() bool {
	parserConfigStruct, _ := parsers.ParserConfigMapToStruct(s.ParserConfig)
	if parserConfigStruct == nil {
		return false
	}
	return parserConfigStruct.IsAppendOnly()
}

func (s *LfSource) Parser() map[string]interface{} {
	return s.ParserConfig
}

func (s *LfSource) MultiYtEnabled() {}
