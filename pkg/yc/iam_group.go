package yc

import (
	"context"
	"crypto/tls"

	iampb "github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/iam/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// IAM provides access to "iam" component of Yandex.Cloud
type IAM struct {
	getConn  func(ctx context.Context) (*grpc.ClientConn, error)
	rpcCreds *rpcCredentials
	creds    Credentials
}

// NewIAM creates instance of IAM
func NewIAM(g func(ctx context.Context) (*grpc.ClientConn, error)) *IAM {
	return &IAM{getConn: g, rpcCreds: nil, creds: nil}
}

// IamToken gets IamTokenService client
func (i *IAM) IamToken() *IamTokenServiceClient {
	return &IamTokenServiceClient{getConn: i.getConn}
}

func (i *IAM) PrivateIamToken() *PrivateIamTokenServiceClient {
	return &PrivateIamTokenServiceClient{getConn: i.getConn}
}

// IamToken gets IamTokenService client
func (i *IAM) ServiceAccounts() *ServiceAccountServiceClient {
	return &ServiceAccountServiceClient{getConn: i.getConn}
}

func (i *IAM) UserAccount() *UserAccountServiceClient {
	return NewUserAccountServiceClient(i.getConn)
}

func (i *IAM) SetCreateToken(createToken createIAMTokenFunc) {
	i.rpcCreds.Init(createToken)
}

func (i *IAM) CreateIAMToken(ctx context.Context) (*iampb.CreateIamTokenResponse, error) {
	creds := i.creds
	switch creds := creds.(type) {
	case ExchangeableCredentials:
		req, err := creds.IAMTokenRequest()
		if err != nil {
			return nil, xerrors.Errorf("cannot create IAM token request: %w", err)
		}
		response, err := i.PrivateIamToken().Create(ctx, req)
		if err != nil {
			return nil, xerrors.Errorf("cannot create token from exchangeable credentials: %w", err)
		}
		return &iampb.CreateIamTokenResponse{IamToken: response.IamToken}, nil
	case NonExchangeableCredentials:
		response, err := creds.IAMToken(ctx)
		if err != nil {
			return nil, xerrors.Errorf("cannot create token from non exchangeable credentials: %w", err)
		}
		return response, nil
	default:
		return nil, xerrors.Errorf("credentials type %T is not supported yet", creds)
	}
}

func NewIAMClient(conf Config, address string, customOpts ...grpc.DialOption) (*IAM, func(ctx context.Context) error) {
	var dialOpts []grpc.DialOption

	dialOpts = append(dialOpts, grpc.WithContextDialer(NewProxyDialer(NewDialer())))

	rpcCreds := newRPCCredentials(conf.Plaintext)
	dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(rpcCreds))
	if conf.Plaintext {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		tlsConfig := conf.TLSConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{}
		}
		creds := credentials.NewTLS(tlsConfig)
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	}
	// Append custom options after default, to allow to customize dialer and etc.
	dialOpts = append(dialOpts, customOpts...)
	cc := NewLazyConnContext(DialOptions(dialOpts...))
	shutdown := func(ctx context.Context) error {
		return cc.Shutdown(ctx)
	}
	return &IAM{
		rpcCreds: rpcCreds,
		creds:    conf.Credentials,
		getConn: func(ctx context.Context) (*grpc.ClientConn, error) {
			return cc.GetConn(ctx, address)
		},
	}, shutdown
}
