package ytclient

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/config/env"
	"github.com/transferia/transferia/pkg/credentials"
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
	ServiceAccountID() string
	ProxyRole() string
}

func FromConnParams(cfg ConnParams, lgr log.Logger) (yt.Client, error) {
	ytConfig := yt.Config{
		Proxy:                    cfg.Proxy(),
		AllowRequestsFromJob:     true,
		CompressionCodec:         yt.ClientCodecBrotliFastest,
		DisableProxyDiscovery:    cfg.DisableProxyDiscovery(),
		UseTLS:                   cfg.UseTLS(),
		CertificateAuthorityData: []byte(cfg.TLSFile()),
		ProxyRole:                cfg.ProxyRole(),
	}
	var cc credentials.Credentials
	var err error
	if cfg.ServiceAccountID() != "" {
		cc, err = credentials.NewServiceAccountCreds(lgr, cfg.ServiceAccountID())
		if err != nil {
			lgr.Error("err", log.Error(err))
			return nil, xerrors.Errorf("cannot init yt client config without credentials client: %w", err)
		}
	}
	ytConfig.CredentialsProviderFn = func(ctx context.Context) (yt.Credentials, error) {
		if env.IsTest() {
			return &yt.TokenCredentials{Token: cfg.Token()}, nil
		}
		if len(cfg.Token()) > 0 {
			return &yt.TokenCredentials{Token: cfg.Token()}, nil
		}
		if cfg.ServiceAccountID() == "" {
			return nil, xerrors.Errorf("unexpected behaviour, it is neccessary that either SA or token is provided")
		}

		if _, err := cc.Token(context.Background()); err != nil {
			lgr.Error("failed resolve token from SA", log.Error(err))
			return nil, xerrors.Errorf("cannot resolve token from %T: %w", cc, err)
		}
		iamToken, err := cc.Token(ctx)
		return &yt.BearerCredentials{Token: iamToken}, err
	}

	if cfg.CompressionCodec() != yt.ClientCodecDefault {
		ytConfig.CompressionCodec = cfg.CompressionCodec()
	}

	return NewYtClientWrapper(HTTP, lgr, &ytConfig)
}
