package yc

import (
	"context"

	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/resourcemanager/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/grpc"
)

type FolderServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (f *FolderServiceClient) Get(ctx context.Context, in *resourcemanager.GetFolderRequest, opts ...grpc.CallOption) (*resourcemanager.Folder, error) {
	conn, err := f.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return resourcemanager.NewFolderServiceClient(conn).Get(ctx, in, opts...)
}
