package yc

import (
	"context"

	iampb "github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/grpc"
)

type PrivateKeyServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *PrivateKeyServiceClient) Create(ctx context.Context, in *iampb.CreateKeyRequest) (*iampb.CreateKeyResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return iampb.NewKeyServiceClient(conn).Create(ctx, in)
}
