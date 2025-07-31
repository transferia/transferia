package yc

import (
	"context"

	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/vpc/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/grpc"
)

type SecurityGroupServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (s *SecurityGroupServiceClient) Get(ctx context.Context, in *vpc.GetSecurityGroupRequest, opts ...grpc.CallOption) (*vpc.SecurityGroup, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return vpc.NewSecurityGroupServiceClient(conn).Get(ctx, in, opts...)
}

func (s *SecurityGroupServiceClient) List(ctx context.Context, in *vpc.ListSecurityGroupsRequest, opts ...grpc.CallOption) (*vpc.ListSecurityGroupsResponse, error) {
	conn, err := s.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return vpc.NewSecurityGroupServiceClient(conn).List(ctx, in, opts...)
}

func (s *VPC) SecurityGroup() *SecurityGroupServiceClient {
	return &SecurityGroupServiceClient{getConn: s.getConn}
}
