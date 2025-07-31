package yc

import (
	"context"

	"google.golang.org/grpc"
)

// Compute provides access to "compute" component of Yandex.Cloud
type Compute struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

// NewCompute creates instance of Compute
func NewCompute(g func(ctx context.Context) (*grpc.ClientConn, error)) *Compute {
	return &Compute{g}
}

// Folder gets FolderService client
func (c *Compute) Image() *ImageServiceClient {
	return &ImageServiceClient{getConn: c.getConn}
}
