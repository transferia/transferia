package logbroker

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/providers/ydb"
)

type LfSource struct {
	Instance             LogbrokerInstance
	Cluster              LogbrokerCluster
	Database             string
	Token                string
	Consumer             string
	MaxReadSize          model.BytesSize
	MaxMemory            model.BytesSize
	MaxTimeLag           time.Duration
	Topics               []string
	MaxIdleTime          time.Duration
	MaxReadMessagesCount uint32
	OnlyLocal            bool
	LfParser             bool
	Credentials          ydb.TokenCredentials
	Port                 int
	AllowTTLRewind       bool

	IsLbSink bool // it's like IsHomo

	ParserConfig map[string]interface{}

	TLS                   TLSMode
	RootCAFiles           []string
	ParseQueueParallelism int

	UsePqv1 bool
}

var _ model.Source = (*LfSource)(nil)

type LogbrokerInstance string
type LogbrokerCluster string

func (s *LfSource) ForceMirror() {}

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
