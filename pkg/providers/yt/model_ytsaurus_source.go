package yt

import (
	"github.com/dustin/go-humanize"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/yt/go/yt"
)

type YTSaurusSource struct {
	Paths []string `log:"true"`

	DesiredPartSizeBytes int64          `log:"true"`
	Connection           ConnectionData `log:"true"`
}

var _ model.Source = (*YTSaurusSource)(nil)

func (s *YTSaurusSource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(s, enc)
}

func (s *YTSaurusSource) IsSource()       {}
func (s *YTSaurusSource) IsStrictSource() {}
func (s *YTSaurusSource) MDBClusterID() string {
	return s.Connection.ClusterID
}

func (s *YTSaurusSource) WithDefaults() {
	if s.DesiredPartSizeBytes == 0 {
		s.DesiredPartSizeBytes = 1 * humanize.GiByte
	}
}

func (s *YTSaurusSource) ServiceAccountIDs() []string {
	if s.Connection.ServiceAccountID == "" {
		return nil
	}
	return []string{s.Connection.ServiceAccountID}
}

func (s *YTSaurusSource) GetProviderType() abstract.ProviderType {
	return ManagedProviderType
}

func (s *YTSaurusSource) Validate() error {
	return nil
}

func (s *YTSaurusSource) IsAbstract2(model.Destination) bool { return true }

func (s *YTSaurusSource) RowIdxEnabled() bool {
	return false
}

func (s *YTSaurusSource) IsAsyncShardPartsSource() {}

func (s *YTSaurusSource) ConnParams() ytclient.ConnParams {
	return s
}

func (s *YTSaurusSource) Proxy() string {
	return proxy(s.Connection.ClusterID)
}

func (s *YTSaurusSource) Token() string {
	return ""
}

func (s *YTSaurusSource) DisableProxyDiscovery() bool {
	return true
}

func (s *YTSaurusSource) CompressionCodec() yt.ClientCompressionCodec {
	return yt.ClientCodecBrotliFastest
}

func (s *YTSaurusSource) UseTLS() bool {
	return s.Connection.UseTLS
}

func (s YTSaurusSource) TLSFile() string {
	return s.Connection.TLSFile
}

func (s YTSaurusSource) ServiceAccountID() string {
	return s.Connection.ServiceAccountID
}

func (s YTSaurusSource) ProxyRole() string {
	return ""
}

func (s *YTSaurusSource) GetPaths() []string {
	return s.Paths
}

func (s *YTSaurusSource) GetDesiredPartSizeBytes() int64 {
	return s.DesiredPartSizeBytes
}

func (s *YTSaurusSource) GetYtToken() string {
	return ""
}

func (s *YTSaurusSource) GetCluster() string {
	return ""
}

func (s *YTSaurusSource) GetRowIdxColumn() string {
	return ""
}
