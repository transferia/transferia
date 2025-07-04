package ytclient

import (
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
)

type ConnParams interface {
	Proxy() string
	Token() string
	DisableProxyDiscovery() bool
	CompressionCodec() yt.ClientCompressionCodec
	UseTLS() bool
	TLSFile() string
}

func FromConnParams(cfg ConnParams, lgr log.Logger) (yt.Client, error) {
	ytConfig := yt.Config{
		Proxy:                    cfg.Proxy(),
		Token:                    cfg.Token(),
		AllowRequestsFromJob:     true,
		CompressionCodec:         yt.ClientCodecBrotliFastest,
		DisableProxyDiscovery:    cfg.DisableProxyDiscovery(),
		UseTLS:                   cfg.UseTLS(),
		CertificateAuthorityData: []byte(cfg.TLSFile()),
	}
	if cfg.CompressionCodec() != yt.ClientCodecDefault {
		ytConfig.CompressionCodec = cfg.CompressionCodec()
	}

	return NewYtClientWrapper(HTTP, lgr, &ytConfig)
}
