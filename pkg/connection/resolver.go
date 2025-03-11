package connection

import (
	"context"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
)

var current ConnResolver

type ManagedConnection interface {
	GetUsername() string
	GetClusterID() string
	GetDatabases() []string
	HostNames() []string
}

var _ ConnResolver = (*UnimplementedResolver)(nil)
var UninitializedResolverErr = xerrors.NewSentinel("connection resolver not initialized!")

type ConnResolver interface {
	ResolveConnection(ctx context.Context, connectionID string, typ abstract.ProviderType) (ManagedConnection, error)
}

func Init(resolver ConnResolver) {
	current = resolver
}

func Resolver() ConnResolver {
	if current != nil {
		return current
	}
	return &UnimplementedResolver{}
}

type UnimplementedResolver struct{}

func (u UnimplementedResolver) ResolveConnection(ctx context.Context, connectionID string, typ abstract.ProviderType) (ManagedConnection, error) {
	return nil, UninitializedResolverErr
}
