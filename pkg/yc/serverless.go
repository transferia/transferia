package yc

import (
	"context"

	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/serverless/functions/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/grpc"
)

type Serverless struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (s *Serverless) ListFunctions(ctx context.Context, in *functions.ListFunctionsRequest) (*functions.ListFunctionsResponse, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return functions.NewFunctionServiceClient(conn).List(ctx, in)
}

func (s *Serverless) Get(ctx context.Context, in *functions.GetFunctionRequest) (*functions.Function, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return functions.NewFunctionServiceClient(conn).Get(ctx, in)
}
