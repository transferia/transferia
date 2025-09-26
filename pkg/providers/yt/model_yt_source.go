package yt

import (
	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/config/env"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"go.ytsaurus.tech/yt/go/yt"
)

type ConnectionData struct {
	Hosts                 []string
	Subnet                string
	SecurityGroups        []string
	DisableProxyDiscovery bool
	UseTLS                bool
	TLSFile               string

	// For YTSaurus only
	ClusterID        string
	ServiceAccountID string
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
	Cluster          string
	YtProxy          string
	Paths            []string
	YtToken          string
	RowIdxColumnName string

	DesiredPartSizeBytes int64
	Connection           ConnectionData
}

var _ model.Source = (*YtSource)(nil)

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
