package yt

import (
	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/config/env"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/yt/go/yt"
)

type ConnectionData struct {
	Hosts                 []string `log:"true"`
	Subnet                string   `log:"true"`
	SecurityGroups        []string `log:"true"`
	DisableProxyDiscovery bool     `log:"true"`
	UseTLS                bool     `log:"true"`
	TLSFile               string
	ProxyRole             string `log:"true"`

	// For YTSaurus only
	ClusterID        string `log:"true"`
	ServiceAccountID string `log:"true"`
}

type YtSourceModel interface {
	ytclient.ConnParams
	model.Source
	model.StrictSource
	model.Abstract2Source
	model.AsyncPartSource

	GetRowIdxColumn() string
	GetPaths() []string
	GetCluster() string
	GetYtToken() string
	GetDesiredPartSizeBytes() int64
}

type YtSource struct {
	Cluster          string   `log:"true"`
	YtProxy          string   `log:"true"`
	Paths            []string `log:"true"`
	YtToken          string
	RowIdxColumnName string `log:"true"`

	DesiredPartSizeBytes int64          `log:"true"`
	Connection           ConnectionData `log:"true"`
}

var _ model.Source = (*YtSource)(nil)

func (s *YtSource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(s, enc)
}

func (s *YtSource) IsSource()       {}
func (s *YtSource) IsStrictSource() {}

func (s *YtSource) WithDefaults() {
	if s.Cluster == "" && env.In(env.EnvironmentInternal) {
		s.Cluster = "hahn"
	}
	if s.DesiredPartSizeBytes == 0 {
		s.DesiredPartSizeBytes = 1 * humanize.GiByte
	}
}

func (s *YtSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *YtSource) Validate() error {
	return nil
}

func (s *YtSource) GetPaths() []string {
	return s.Paths
}

func (s *YtSource) GetDesiredPartSizeBytes() int64 {
	return s.DesiredPartSizeBytes
}

func (s *YtSource) GetYtToken() string {
	return s.YtToken
}

func (s *YtSource) GetCluster() string {
	return s.Cluster
}

func (s *YtSource) GetRowIdxColumn() string {
	return s.RowIdxColumnName
}

func (s *YtSource) IsAbstract2(model.Destination) bool { return true }

func (s *YtSource) IsAsyncShardPartsSource() {}

func (s YtSource) Proxy() string {
	if s.YtProxy != "" {
		return s.YtProxy
	}
	return s.Cluster
}

func (s *YtSource) Token() string {
	return s.YtToken
}

func (s *YtSource) DisableProxyDiscovery() bool {
	return s.Connection.DisableProxyDiscovery
}

func (s *YtSource) CompressionCodec() yt.ClientCompressionCodec {
	return yt.ClientCodecBrotliFastest
}

func (s *YtSource) UseTLS() bool {
	return s.Connection.UseTLS
}

func (s *YtSource) TLSFile() string {
	return s.Connection.TLSFile
}

func (s *YtSource) ServiceAccountID() string {
	return ""
}

func (s *YtSource) ProxyRole() string {
	return s.Connection.ProxyRole
}
