package yc

import (
	"context"

	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/access"
	iampb "github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/iam/v1"
	operation "github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/operation"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/grpc"
)

type ServiceAccountServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *ServiceAccountServiceClient) Get(ctx context.Context, in *iampb.GetServiceAccountRequest, opts ...grpc.CallOption) (*iampb.ServiceAccount, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return iampb.NewServiceAccountServiceClient(conn).Get(ctx, in, opts...)
}

func (c *ServiceAccountServiceClient) List(ctx context.Context, in *iampb.ListServiceAccountsRequest, opts ...grpc.CallOption) (*iampb.ListServiceAccountsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return iampb.NewServiceAccountServiceClient(conn).List(ctx, in, opts...)
}

func (c *ServiceAccountServiceClient) Create(ctx context.Context, in *iampb.CreateServiceAccountRequest, opts ...grpc.CallOption) (*operation.Operation, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return iampb.NewServiceAccountServiceClient(conn).Create(ctx, in, opts...)
}

func (c *ServiceAccountServiceClient) SetAccessBindings(ctx context.Context, in *access.SetAccessBindingsRequest, opts ...grpc.CallOption) (*operation.Operation, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return iampb.NewServiceAccountServiceClient(conn).SetAccessBindings(ctx, in, opts...)
}
