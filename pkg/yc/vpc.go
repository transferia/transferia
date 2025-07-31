package yc

import (
	"context"

	iampb "github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/operation"
	"github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/vpc/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/auth/resources"
	"google.golang.org/grpc"
)

type SubnetServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

// Create implements vpc.SubnetServiceClient
func (c *SubnetServiceClient) Create(ctx context.Context, in *vpc.CreateSubnetRequest, opts ...grpc.CallOption) (*operation.Operation, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return vpc.NewSubnetServiceClient(conn).Create(ctx, in, opts...)
}

// Delete implements vpc.SubnetServiceClient
func (c *SubnetServiceClient) Delete(ctx context.Context, in *vpc.DeleteSubnetRequest, opts ...grpc.CallOption) (*operation.Operation, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return vpc.NewSubnetServiceClient(conn).Delete(ctx, in, opts...)
}

// Get implements vpc.SubnetServiceClient
func (c *SubnetServiceClient) Get(ctx context.Context, in *vpc.GetSubnetRequest, opts ...grpc.CallOption) (*vpc.Subnet, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return vpc.NewSubnetServiceClient(conn).Get(ctx, in, opts...)
}

// List implements vpc.SubnetServiceClient
func (c *SubnetServiceClient) List(ctx context.Context, in *vpc.ListSubnetsRequest, opts ...grpc.CallOption) (*vpc.ListSubnetsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return vpc.NewSubnetServiceClient(conn).List(ctx, in, opts...)
}

type SubnetIterator struct {
	ctx  context.Context
	opts []grpc.CallOption

	err     error
	started bool

	client  *SubnetServiceClient
	request *vpc.ListSubnetsRequest

	items []*vpc.Subnet
}

func (c *SubnetServiceClient) SubnetIterator(ctx context.Context, folderID string, opts ...grpc.CallOption) *SubnetIterator {
	return &SubnetIterator{
		ctx:     ctx,
		opts:    opts,
		err:     nil,
		started: false,
		client:  c,
		request: &vpc.ListSubnetsRequest{
			FolderId: folderID,
			PageSize: 1000,
		},
		items: []*vpc.Subnet{},
	}
}

func (it *SubnetIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if len(it.items) > 1 {
		it.items[0] = nil
		it.items = it.items[1:]
		return true
	}
	it.items = nil // consume last item, if any

	if it.started && it.request.PageToken == "" {
		return false
	}
	it.started = true

	response, err := it.client.List(it.ctx, it.request, it.opts...)
	it.err = err
	if err != nil {
		return false
	}

	it.items = response.Subnets
	it.request.PageToken = response.NextPageToken
	return len(it.items) > 0
}

func (it *SubnetIterator) Value() *vpc.Subnet {
	if len(it.items) == 0 {
		panic("calling Value on empty iterator")
	}
	return it.items[0]
}

func (it *SubnetIterator) Error() error {
	return it.err
}

// ListOperations implements vpc.SubnetServiceClient
func (c *SubnetServiceClient) ListOperations(ctx context.Context, in *vpc.ListSubnetOperationsRequest, opts ...grpc.CallOption) (*vpc.ListSubnetOperationsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return vpc.NewSubnetServiceClient(conn).ListOperations(ctx, in, opts...)
}

type SubnetOperationsIterator struct {
	ctx  context.Context
	opts []grpc.CallOption

	err     error
	started bool

	client  *SubnetServiceClient
	request *vpc.ListSubnetOperationsRequest

	items []*operation.Operation
}

func (c *SubnetServiceClient) SubnetOperationsIterator(ctx context.Context, subnetID string, opts ...grpc.CallOption) *SubnetOperationsIterator {
	return &SubnetOperationsIterator{
		ctx:     ctx,
		opts:    opts,
		err:     nil,
		started: false,
		client:  c,
		request: &vpc.ListSubnetOperationsRequest{
			SubnetId: subnetID,
			PageSize: 1000,
		},
		items: nil,
	}
}

func (it *SubnetOperationsIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if len(it.items) > 1 {
		it.items[0] = nil
		it.items = it.items[1:]
		return true
	}
	it.items = nil // consume last item, if any

	if it.started && it.request.PageToken == "" {
		return false
	}
	it.started = true

	response, err := it.client.ListOperations(it.ctx, it.request, it.opts...)
	it.err = err
	if err != nil {
		return false
	}

	it.items = response.Operations
	it.request.PageToken = response.NextPageToken
	return len(it.items) > 0
}

func (it *SubnetOperationsIterator) Value() *operation.Operation {
	if len(it.items) == 0 {
		panic("calling Value on empty iterator")
	}
	return it.items[0]
}

func (it *SubnetOperationsIterator) Error() error {
	return it.err
}

// Move implements vpc.SubnetServiceClient
func (c *SubnetServiceClient) Move(ctx context.Context, in *vpc.MoveSubnetRequest, opts ...grpc.CallOption) (*operation.Operation, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return vpc.NewSubnetServiceClient(conn).Move(ctx, in, opts...)
}

// Update implements vpc.SubnetServiceClient
func (c *SubnetServiceClient) Update(ctx context.Context, in *vpc.UpdateSubnetRequest, opts ...grpc.CallOption) (*operation.Operation, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return vpc.NewSubnetServiceClient(conn).Update(ctx, in, opts...)
}

type NetworkServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (c *NetworkServiceClient) ListSecurityGroups(ctx context.Context, in *vpc.ListNetworkSecurityGroupsRequest, opts ...grpc.CallOption) (*vpc.ListNetworkSecurityGroupsResponse, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return WithAgentFromOpts(ctx, vpc.NewNetworkServiceClient(conn).ListSecurityGroups, in, &iampb.Resource{
		Id:   in.GetNetworkId(),
		Type: resources.ResourceNetwork,
	}, opts...)
}

func (c *NetworkServiceClient) Get(ctx context.Context, in *vpc.GetNetworkRequest, opts ...grpc.CallOption) (*vpc.Network, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("cannot get connection: %w", err)
	}
	return vpc.NewNetworkServiceClient(conn).Get(ctx, in, opts...)
}

type VPC struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func NewVPC(g func(ctx context.Context) (*grpc.ClientConn, error)) *VPC {
	return &VPC{g}
}

func (s *VPC) Subnet() *SubnetServiceClient {
	return &SubnetServiceClient{getConn: s.getConn}
}

func (s *VPC) Network() *NetworkServiceClient {
	return &NetworkServiceClient{getConn: s.getConn}
}
