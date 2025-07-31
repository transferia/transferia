package yc

import (
	"context"

	"github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/access"
	iampb "github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	"github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/operation"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/grpc"
)

type PrivateServiceAccountServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *PrivateServiceAccountServiceClient) Get(ctx context.Context, in *iampb.GetServiceAccountRequest, opts ...grpc.CallOption) (*iampb.ServiceAccount, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return iampb.NewServiceAccountServiceClient(conn).Get(ctx, in, opts...)
}

func (c *PrivateServiceAccountServiceClient) List(ctx context.Context, in *iampb.ListServiceAccountsRequest, opts ...grpc.CallOption) (*iampb.ListServiceAccountsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return iampb.NewServiceAccountServiceClient(conn).List(ctx, in, opts...)
}

func (c *PrivateServiceAccountServiceClient) Create(ctx context.Context, in *iampb.CreateServiceAccountRequest, opts ...grpc.CallOption) (*operation.Operation, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return iampb.NewServiceAccountServiceClient(conn).Create(ctx, in, opts...)
}

func (c *PrivateServiceAccountServiceClient) Delete(ctx context.Context, in *iampb.DeleteServiceAccountRequest, opts ...grpc.CallOption) (*operation.Operation, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return iampb.NewServiceAccountServiceClient(conn).Delete(ctx, in, opts...)
}

func (c *PrivateServiceAccountServiceClient) SetAccessBindings(ctx context.Context, in *access.SetAccessBindingsRequest, opts ...grpc.CallOption) (*operation.Operation, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return iampb.NewServiceAccountServiceClient(conn).SetAccessBindings(ctx, in, opts...)
}

func (c *PrivateServiceAccountServiceClient) ListAccessBindings(ctx context.Context, in *access.ListAccessBindingsRequest, opts ...grpc.CallOption) (*access.ListAccessBindingsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return iampb.NewServiceAccountServiceClient(conn).ListAccessBindings(ctx, in, opts...)
}
