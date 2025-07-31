package token

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/grpcutil"
	"github.com/transferia/transferia/pkg/stringutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type InterceptorConfig struct {
	Skip               func(method string) bool
	AuthorizationMDKey string
	OAuthTokenPrefix   []string
	IamTokenPrefix     []string
	ClientIPExtractor  func(ctx context.Context) string
}

func NewInterceptorConfig(skip func(method string) bool, clientIPExtractor func(ctx context.Context) string) *InterceptorConfig {
	if clientIPExtractor == nil {
		clientIPExtractor = func(context.Context) string { return "" }
	}
	return &InterceptorConfig{
		Skip:               skip,
		AuthorizationMDKey: "authorization",
		OAuthTokenPrefix:   []string{"oauth "},
		IamTokenPrefix:     []string{"iam ", "bearer "},
		ClientIPExtractor:  clientIPExtractor,
	}
}

var (
	_ grpcutil.UnaryServerInterceptor  = (*interceptor)(nil)
	_ grpcutil.StreamServerInterceptor = (*interceptor)(nil)
)

type interceptor struct {
	config   *InterceptorConfig
	provider Provider
}

func NewInterceptor(config *InterceptorConfig, provider Provider) *interceptor {
	return &interceptor{config: config, provider: provider}
}

func (i *interceptor) Intercept(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if i.config.Skip == nil || !i.config.Skip(info.FullMethod) {
		var err error
		ctx, err = i.intercept(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "interception failed: %v", err)
		}
	}

	return handler(ctx, req)
}

func (i *interceptor) InterceptStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()
	if i.config.Skip == nil || !i.config.Skip(info.FullMethod) {
		var err error
		ctx, err = i.intercept(ctx)
		if err != nil {
			return status.Errorf(codes.Unauthenticated, "stream interception failed: %v", err)
		}
	}

	wrappedStream := &grpc_middleware.WrappedServerStream{ServerStream: ss, WrappedContext: ctx}
	return handler(srv, wrappedStream)
}

func (i *interceptor) intercept(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, xerrors.New("unable to get incoming metadata")
	}

	authorizationMD, ok := md[i.config.AuthorizationMDKey]
	if !ok {
		return nil, xerrors.New("unable to get authorization metadata")
	}
	if len(authorizationMD) == 0 {
		return nil, xerrors.New("authorization metadata is empty")
	}
	authorization := authorizationMD[0]

	token, ok := stringutil.TrimPrefixCI(authorization, i.config.IamTokenPrefix...)
	if !ok {
		oauthToken, ok := stringutil.TrimPrefixCI(authorization, i.config.OAuthTokenPrefix...)
		if !ok {
			oauthToken = authorization
		}

		var err error
		clientIP := i.config.ClientIPExtractor(ctx)
		token, err = i.provider.CreateToken(ctx, oauthToken, clientIP)
		if err != nil {
			return nil, xerrors.Errorf("unable to create token: %w", err)
		}
	}

	ctx = WithToken(ctx, token)
	return ctx, nil
}
