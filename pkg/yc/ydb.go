package yc

import (
	"context"

	iampb "github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	ydbpb "github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/ydb/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/auth/resources"
	"github.com/transferia/transferia/pkg/errors/coded"
	grpcutil "github.com/transferia/transferia/pkg/util/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var YDBNotFoundCode = coded.Register("ydb", "not_found")

type YDB struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *YDB) List(ctx context.Context, in *ydbpb.ListDatabasesRequest, opts ...grpc.CallOption) (*ydbpb.ListDatabasesResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return ydbpb.NewDatabaseServiceClient(conn).List(ctx, in, opts...)
}

func (c *YDB) GetDatabase(ctx context.Context, in *ydbpb.GetDatabaseRequest, opts ...grpc.CallOption) (*ydbpb.Database, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	db, err := WithAgentFromOpts(ctx, ydbpb.NewDatabaseServiceClient(conn).Get, in, &iampb.Resource{
		Id:   in.GetDatabaseId(),
		Type: resources.ResourceYdb,
	}, opts...)
	if err != nil {
		if ok, statusErr := grpcutil.UnwrapStatusError(err); ok && statusErr.GRPCStatus().Code() == codes.NotFound {
			return nil, coded.Errorf(YDBNotFoundCode, "cluster does not exist: %w", statusErr)
		}
		return nil, xerrors.Errorf("Yandex cloud error: %w", err)
	}
	return db, nil
}
