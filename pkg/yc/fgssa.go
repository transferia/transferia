package yc

import (
	"context"

	iampb "github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/auth/fgssa"
	"github.com/transferia/transferia/pkg/token"
	"google.golang.org/grpc"
)

type IssueAgentTokenOpt struct {
	grpc.EmptyCallOption
}

type IssueAgentTokenOptForDP struct {
	grpc.EmptyCallOption
}

func WithIssueAgentToken() grpc.CallOption {
	return IssueAgentTokenOpt{
		EmptyCallOption: grpc.EmptyCallOption{},
	}
}

func WithIssueAgentTokenForDP() grpc.CallOption {
	return IssueAgentTokenOptForDP{
		EmptyCallOption: grpc.EmptyCallOption{},
	}
}

type callFunc[Request any, Response any] func(ctx context.Context, in Request, opts ...grpc.CallOption) (Response, error)

func WithAgentFromOpts[Request any, Response any](
	ctx context.Context,
	callFunc callFunc[Request, Response],
	in Request,
	resource *iampb.Resource,
	opts ...grpc.CallOption,
) (Response, error) {
	var nilResponse Response

	agentToken, err := tokenFromOpts(ctx, resource, opts...)
	if err != nil {
		return nilResponse, xerrors.Errorf("cannot create agent token from opts: %w", err)
	}

	if agentToken != "" {
		ctx = WithUserAuth(token.WithToken(ctx, agentToken))
	}

	return callFunc(ctx, in, opts...)
}

func tokenFromOpts(ctx context.Context, resource *iampb.Resource, opts ...grpc.CallOption) (string, error) {
	for _, opt := range opts {
		switch opt.(type) {
		case IssueAgentTokenOpt:
			tokenIssuer, err := fgssa.TokenIssuerInstance()
			if err != nil {
				return "", xerrors.Errorf("cannot create agent token issuer: %w", err)
			}
			token, err := tokenIssuer.Token(ctx, resource)
			if err != nil {
				return "", xerrors.Errorf("cannot create agent token: %w", err)
			}
			return token, nil
		case IssueAgentTokenOptForDP:
			if !fgssa.UseFineGrainedSSA() {
				return "", nil
			}
			token, err := fgssa.NewAgentTokenCredentials(resource.GetId(), resource.GetType()).Token(ctx)
			if err != nil {
				return "", xerrors.Errorf("cannot create agent token for dataplane: %w", err)
			}
			return token, nil
		}
	}

	return "", nil
}
