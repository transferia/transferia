package yc

import (
	"context"

	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/compute/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/grpc"
)

type ImageServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (i *ImageServiceClient) Get(ctx context.Context, in *compute.GetImageRequest, opts ...grpc.CallOption) (*compute.Image, error) {
	conn, err := i.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return compute.NewImageServiceClient(conn).Get(ctx, in, opts...)
}
