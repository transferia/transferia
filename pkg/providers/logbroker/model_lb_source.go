package logbroker

import (
	"strings"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/ydb"
	"go.uber.org/zap/zapcore"
)

type LbSource struct {
	Instance       string `log:"true"`
	Topic          string `log:"true"`
	Token          string
	Consumer       string `log:"true"`
	Database       string `log:"true"`
	AllowTTLRewind bool   `log:"true"`
	Credentials    ydb.TokenCredentials
	Port           int `log:"true"`

	IsLbSink bool `log:"true"` // it's like IsHomo

	RootCAFiles    []string
	TLS            TLSMode                `log:"true"`
	ParserConfig   map[string]interface{} // not used, just for api consistency
	ReadOptionsSet bool                   // for api consistency
}

var _ model.Source = (*LbSource)(nil)

const (
	Logbroker            LogbrokerCluster = "logbroker"
	Lbkx                 LogbrokerCluster = "lbkx"
	Messenger            LogbrokerCluster = "messenger"
	LogbrokerPrestable   LogbrokerCluster = "logbroker-prestable"
	Lbkxt                LogbrokerCluster = "lbkxt"
	YcLogbroker          LogbrokerCluster = "yc-logbroker"
	YcLogbrokerPrestable LogbrokerCluster = "yc-logbroker-prestable"
)

func (s *LbSource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(s, enc)
}

func (s *LbSource) WithDefaults() {
}

func (LbSource) IsSource() {
}

func (s *LbSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *LbSource) Validate() error {
	return nil
}

func (s *LbSource) IsTransitional() {}

func (s *LbSource) TransitionalWith(right model.TransitionalEndpoint) bool {
	if dst, ok := right.(*LbDestination); ok {
		return dst.Instance == s.Instance && dst.Topic == s.Topic
	}
	return false
}

func (s *LbSource) MultiYtEnabled() {}

func withoutLeadingSlash(str string) string {
	if strings.HasPrefix(str, "/") {
		return str[1:]
	}
	return str
}
