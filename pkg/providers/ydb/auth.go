package ydb

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/credentials"
	v3credential "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"go.ytsaurus.tech/library/go/core/log"
)

// TokenCredentials is an interface that contains options used to authorize a
// client.
type TokenCredentials interface {
	Token(context.Context) (string, error)
}

var JWTCredentials = func(content string, tokenServiceURL string) (TokenCredentials, error) {
	return nil, xerrors.Errorf("not implemented")
}

// Credentials is an abstraction of API authorization credentials.
// See https://cloud.yandex.ru/docs/iam/concepts/authorization/authorization for details.
// Note that functions that return Credentials may return different Credentials implementation
// in next SDK version, and this is not considered breaking change.
type Credentials interface {
	// YandexCloudAPICredentials is a marker method. All compatible Credentials implementations have it
	YandexCloudAPICredentials()
}

var NewYDBCredsFromYCCreds = func(ycCreds Credentials, tokenService string) TokenCredentials {
	return nil
}

type JWTAuthParams struct {
	KeyContent      string
	TokenServiceURL string
}

func ResolveCredentials(
	userDataAuth bool,
	oauthToken string,
	jwt JWTAuthParams,
	serviceAccountID string,
	oauthConfig *v3credential.OAuth2Config,
	logger log.Logger,
) (TokenCredentials, error) {
	if serviceAccountID != "" {
		cc, err := credentials.NewServiceAccountCreds(logger, serviceAccountID)
		if err != nil {
			logger.Error("err", log.Error(err))
			return nil, xerrors.Errorf("cannot init kinesis reader config without credentials client: %w", err)
		}
		logger.Infof("try SA account: %v", serviceAccountID)
		if _, err := cc.Token(context.Background()); err != nil {
			logger.Error("failed resolve token from SA", log.Error(err))
			return nil, xerrors.Errorf("cannot resolve token from %T: %w", cc, err)
		}
		logger.Infof("bind SA account: %v", serviceAccountID)
		return cc, nil
	}
	if oauthToken != "" {
		cc := credentials.NewStaticCreds(oauthToken)
		return cc, nil
	}
	if len(jwt.KeyContent) > 0 {
		cc, err := JWTCredentials(jwt.KeyContent, jwt.TokenServiceURL)
		if err != nil {
			return nil, xerrors.Errorf("cannot create jwt token: %w", err)
		}
		return cc, nil
	}
	if oauthConfig != nil {
		opts, err := oauthConfig.AsOptions()
		if err != nil {
			return nil, xerrors.Errorf("connot extract oauth2 options: %w", err)
		}
		cc, err := v3credential.NewOauth2TokenExchangeCredentials(opts...)
		if err != nil {
			return nil, xerrors.Errorf("cannot create oauth credentials: %w", err)
		}
		return cc, nil
	}
	if userDataAuth {
		return credentials.NewIamCreds(logger)
	}
	return v3credential.NewAnonymousCredentials(), nil
}
