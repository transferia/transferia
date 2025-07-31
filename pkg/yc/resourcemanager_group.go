package yc

import (
	"context"

	"google.golang.org/grpc"
)

// ResourceManager provides access to "resourcemanager" component of Yandex.Cloud
type ResourceManager struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

// NewResourceManager creates instance of ResourceManager
func NewResourceManager(g func(ctx context.Context) (*grpc.ClientConn, error)) *ResourceManager {
	return &ResourceManager{g}
}

// Folder gets FolderService client
func (rm *ResourceManager) Folder() *FolderServiceClient {
	return &FolderServiceClient{getConn: rm.getConn}
}
