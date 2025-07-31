package yc

import (
	"context"

	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/operation"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/grpc"
)

// InstanceGroupServiceClient is a instancegroup.InstanceGroupServiceClient with
// lazy GRPC connection initialization.
type OperationServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

// Get implements instancegroup.InstanceGroupServiceClient
func (c *OperationServiceClient) Get(ctx context.Context, in *operation.GetOperationRequest, opts ...grpc.CallOption) (*operation.Operation, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return operation.NewOperationServiceClient(conn).Get(ctx, in, opts...)
}
