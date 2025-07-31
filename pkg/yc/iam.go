package yc

import (
	"context"

	"github.com/cenkalti/backoff/v4"
	privateiam "github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/iam/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CreateIamTokenResponse is minimal interface for token
// It is implemented by both public and private iam.CreateIamTokenResponse
type CreateIamTokenResponse interface {
	GetIamToken() string
	GetExpiresAt() *timestamppb.Timestamp
}

// IamTokenServiceClient is a iam.IamTokenServiceClient with
// lazy GRPC connection initialization.
type IamTokenServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

// Create implements iam.IamTokenServiceClient
func (c *IamTokenServiceClient) Create(ctx context.Context, in *iam.CreateIamTokenRequest, opts ...grpc.CallOption) (*iam.CreateIamTokenResponse, error) {
	return backoff.RetryWithData(func() (*iam.CreateIamTokenResponse, error) {
		conn, err := c.getConn(ctx)
		if err != nil {
			return nil, xerrors.Errorf("cannot get connection: %w", err)
		}
		response, iamErr := iam.NewIamTokenServiceClient(conn).Create(ctx, in, opts...)
		if code := status.Code(iamErr); code == codes.PermissionDenied || code == codes.DeadlineExceeded || code == codes.Canceled {
			return nil, backoff.Permanent(xerrors.Errorf("cannot create token, not retriable: %w", iamErr))
		}
		if iamErr != nil {
			return nil, xerrors.Errorf("cannot create token: %w", iamErr)
		}
		return response, nil
	}, backoff.WithMaxRetries(backoff.WithContext(backoff.NewExponentialBackOff(), ctx), 5))
}

// CreateForServiceAccount implements iam.IamTokenServiceClient
func (c *IamTokenServiceClient) CreateForServiceAccount(ctx context.Context, in *iam.CreateIamTokenForServiceAccountRequest) (*iam.CreateIamTokenResponse, error) {
	return backoff.RetryWithData(func() (*iam.CreateIamTokenResponse, error) {
		conn, err := c.getConn(ctx)
		if err != nil {
			return nil, xerrors.Errorf("cannot get connection: %w", err)
		}
		response, iamErr := iam.NewIamTokenServiceClient(conn).CreateForServiceAccount(ctx, in)
		if code := status.Code(iamErr); code == codes.PermissionDenied || code == codes.DeadlineExceeded || code == codes.Canceled {
			return nil, backoff.Permanent(xerrors.Errorf("cannot create token for SA: %w", iamErr))
		}
		if iamErr != nil {
			return nil, xerrors.Errorf("cannot create token for SA: %w", iamErr)
		}
		return response, nil
	}, backoff.WithMaxRetries(backoff.WithContext(backoff.NewExponentialBackOff(), ctx), 5))
}

type PrivateIamTokenServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *PrivateIamTokenServiceClient) Create(ctx context.Context, in *iam.CreateIamTokenRequest, opts ...grpc.CallOption) (*privateiam.CreateIamTokenResponse, error) {
	return backoff.RetryWithData(func() (*privateiam.CreateIamTokenResponse, error) {
		conn, err := c.getConn(ctx)
		if err != nil {
			return nil, xerrors.Errorf("cannot get connection: %w", err)
		}
		privateReq := &privateiam.CreateIamTokenRequest{}
		if err := util.MapProtoJSON(in, privateReq); err != nil {
			//nolint:descriptiveerrors
			return nil, backoff.Permanent(xerrors.Errorf("cannot create iam token request from JSON: %w", err))
		}
		response, iamErr := privateiam.NewIamTokenServiceClient(conn).Create(ctx, privateReq, opts...)
		if code := status.Code(iamErr); code == codes.PermissionDenied || code == codes.DeadlineExceeded || code == codes.Canceled {
			//nolint:descriptiveerrors
			return nil, backoff.Permanent(xerrors.Errorf("cannot create token, not retriable: %w", iamErr))
		}
		if iamErr != nil {
			return nil, xerrors.Errorf("cannot create token: %w", iamErr)
		}
		return response, nil
	}, backoff.WithMaxRetries(backoff.WithContext(backoff.NewExponentialBackOff(), ctx), 5))
}
