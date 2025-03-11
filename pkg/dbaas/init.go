package dbaas

import "github.com/transferria/transferria/library/go/core/xerrors"

var current ResolverFactory

func Current() (ResolverFactory, error) {
	if current == nil {
		return nil, xerrors.New("dbaas provider not initialized")
	}
	return current, nil
}

func ResolveClusterHosts(typ ProviderType, cluster string) ([]ClusterHost, error) {
	if current == nil {
		return nil, xerrors.New("dbaas provider not initialized")
	}
	resolver, err := current.HostResolver(typ, cluster)
	if err != nil {
		return nil, xerrors.Errorf("unable to create resolver for: %s: %s: %w", typ, cluster, err)
	}
	return resolver.ResolveHosts()
}

func Init(provider ResolverFactory) {
	current = provider
}
