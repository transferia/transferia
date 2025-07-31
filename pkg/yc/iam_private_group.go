package yc

import (
	"context"
	"crypto/tls"

	iampb "github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/iam/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// PrivateIAM provides access to "iam" private APIs
type PrivateIAM struct {
	getConn      func(ctx context.Context) (*grpc.ClientConn, error)
	rpcCreds     *rpcCredentials
	serviceCreds Credentials
	shutdown     func(ctx context.Context) error
}

func (i *PrivateIAM) Shutdown(ctx context.Context) error {
	return i.shutdown(ctx)
}

func (i *PrivateIAM) PrivateIamToken() *PrivateIamTokenServiceClient {
	return &PrivateIamTokenServiceClient{getConn: i.getConn}
}

func (i *PrivateIAM) PrivateKey() *PrivateKeyServiceClient {
	return &PrivateKeyServiceClient{getConn: i.getConn}
}

func (i *PrivateIAM) PrivateServiceAccounts() *PrivateServiceAccountServiceClient {
	return &PrivateServiceAccountServiceClient{getConn: i.getConn}
}

func NewPrivateIAMClient(privateURL, tokenURL string, serviceCreds Credentials, customOpts ...grpc.DialOption) (*PrivateIAM, error) {
	var dialOpts []grpc.DialOption

	rpcCreds := newRPCCredentials(false)
	dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(rpcCreds))
	dialOpts = append(dialOpts, grpc.WithContextDialer(NewProxyDialer(NewDialer())))
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(new(tls.Config))))
	// Append custom options after default, to allow to customize dialer etc.
	dialOpts = append(dialOpts, customOpts...)
	cc := NewLazyConnContext(DialOptions(dialOpts...))
	shutdown := func(ctx context.Context) error {
		return cc.Shutdown(ctx)
	}
	res := &PrivateIAM{
		rpcCreds:     rpcCreds,
		serviceCreds: serviceCreds,
		shutdown:     shutdown,
		getConn: func(ctx context.Context) (*grpc.ClientConn, error) {
			return cc.GetConn(ctx, privateURL)
		},
	}
	res.rpcCreds.Init(func(ctx context.Context) (*iampb.CreateIamTokenResponse, error) {
		switch creds := serviceCreds.(type) {
		case ExchangeableCredentials:
			req, err := creds.IAMTokenRequest()
			if err != nil {
				return nil, xerrors.Errorf("unable to init IAM token request from cloud creds: %w", err)
			}
			client, shutdown := NewIAMClient(DefaultConfig(), tokenURL)
			defer shutdown(ctx)
			tkn, err := client.PrivateIamToken().Create(ctx, req)
			if err != nil {
				return nil, xerrors.Errorf("unable to create token: %w", err)
			}
			return &iampb.CreateIamTokenResponse{IamToken: tkn.IamToken}, nil
		case NonExchangeableCredentials:
			response, err := creds.IAMToken(ctx)
			if err != nil {
				return nil, xerrors.Errorf("cannot create token from non exchangeable credentials: %w", err)
			}
			return response, nil
		default:
			return nil, xerrors.Errorf("credentials type %T is not supported yet", creds)
		}
	})
	return res, nil
}
