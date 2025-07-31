package token

import (
	"context"
	"crypto/tls"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/cleanup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Provider interface {
	cleanup.Closeable
	CreateToken(ctx context.Context, oauthToken, clientIP string) (string, error)
}

type ClientConfig struct {
	Endpoint string `mapstructure:"endpoint"`
}

type ProviderConfig struct {
	Client ClientConfig `mapstructure:"client"`
}

type provider struct {
	client iam.IamTokenServiceClient
	conn   *grpc.ClientConn
}

func NewProvider(ctx context.Context, config *ProviderConfig) (Provider, error) {
	client, conn, err := newClient(ctx, &config.Client)
	if err != nil {
		return nil, xerrors.Errorf("unable to initialize client: %w", err)
	}
	return &provider{client: client, conn: conn}, nil
}

func (p *provider) Close() error {
	return p.conn.Close()
}

func (p *provider) CreateToken(ctx context.Context, oauthToken, clientIP string) (string, error) {
	request := &iam.CreateIamTokenRequest{
		Identity: &iam.CreateIamTokenRequest_YandexPassportOauthToken{
			YandexPassportOauthToken: oauthToken,
		},
	}
	if clientIP != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "X-User-Ip", clientIP)
	}

	response, err := backoff.RetryWithData(func() (*iam.CreateIamTokenResponse, error) {
		resp, iamErr := p.client.Create(ctx, request)
		if code := status.Code(iamErr); code == codes.PermissionDenied || code == codes.DeadlineExceeded || code == codes.Canceled {
			//nolint:descriptiveerrors
			return nil, backoff.Permanent(xerrors.Errorf("cannot create token, not retriable: %w", iamErr))
		}
		if iamErr != nil {
			//nolint:descriptiveerrors
			return nil, iamErr
		}
		return resp, nil
	}, backoff.WithMaxRetries(backoff.WithContext(backoff.NewExponentialBackOff(), ctx), 5))
	if err != nil {
		return "", status.Errorf(codes.Internal, "cannot create token after 5 retries: %v", err.Error())
	}
	return response.IamToken, nil
}

func newClient(ctx context.Context, config *ClientConfig) (iam.IamTokenServiceClient, *grpc.ClientConn, error) {
	creds := credentials.NewTLS(new(tls.Config))
	conn, err := grpc.DialContext(ctx, config.Endpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to connect: %w", err)
	}
	client := iam.NewIamTokenServiceClient(conn)
	return client, conn, nil
}
