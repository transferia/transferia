package dbaas

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

var current ResolverFactory

func Current() (ResolverFactory, error) {
	if current == nil {
		return nil, xerrors.New("dbaas provider not initialized")
	}
	return current, nil
}

func ResolveClusterHosts(ctx context.Context, typ ProviderType, cluster string) ([]ClusterHost, error) {
	if current == nil {
		return nil, xerrors.New("dbaas provider not initialized")
	}
	resolver, err := current.HostResolver(ctx, typ, cluster)
	if err != nil {
		return nil, xerrors.Errorf("unable to create resolver for: %s: %s: %w", typ, cluster, err)
	}
	return resolver.ResolveHosts(ctx)
}

func Init(provider ResolverFactory) {
	current = provider
}
