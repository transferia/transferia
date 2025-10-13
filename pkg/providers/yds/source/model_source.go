package source

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/providers/ydb"
	ydstype "github.com/transferia/transferia/pkg/providers/yds/type"
)

type YDSSource struct {
	Endpoint         string
	Database         string
	Stream           string
	Consumer         string
	S3BackupBucket   string `model:"ObjectStorageBackupBucket"`
	Port             int
	BackupMode       model.BackupMode
	Transformer      *model.DataTransformOptions
	SubNetworkID     string
	SecurityGroupIDs []string
	SupportedCodecs  []YdsCompressionCodec // TODO: Replace with pq codecs?
	AllowTTLRewind   bool

	IsLbSink bool // it's like IsHomo

	TLSEnalbed  bool
	RootCAFiles []string

	ParserConfig map[string]interface{}
	Underlay     bool

	// Auth properties
	Credentials           ydb.TokenCredentials
	ServiceAccountID      string `model:"ServiceAccountId"`
	SAKeyContent          string
	TokenServiceURL       string
	Token                 model.SecretString
	UserdataAuth          bool
	ParseQueueParallelism int
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

func (s *YDSSource) MDBClusterID() string {
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
	return ydstype.ProviderType
}

func (s *YDSSource) Validate() error {
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

func (s *YDSSource) IsDefaultMirror() bool {
	return s.ParserConfig == nil
}

func (s *YDSSource) Parser() map[string]interface{} {
	return s.ParserConfig
}
