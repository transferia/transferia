package yc

import (
	"context"

	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/storage/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/grpc"
)

type Storage struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (s *Storage) GetBucket(ctx context.Context, in *storage.GetBucketRequest) (*storage.Bucket, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return storage.NewBucketServiceClient(conn).Get(ctx, in)
}
