//go:build !disable_clickhouse_provider

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/dbaas"
)

type mockResolveInterface interface {
	dbaas.ResolverFactory
	dbaas.HostResolver
	dbaas.PasswordResolver
	dbaas.ShardResolver
	dbaas.ShardGroupHostsResolver
}

type mockResolver struct {
	mockResolveInterface
}

func (r *mockResolver) HostResolver(typ dbaas.ProviderType, clusterID string) (dbaas.HostResolver, error) {
	if typ != dbaas.ProviderTypeClickhouse {
		return nil, xerrors.New("unsupported provider type")
	}
	return r, nil
}

func (r *mockResolver) ShardGroupHostsResolver(typ dbaas.ProviderType, clusterID string) (dbaas.ShardGroupHostsResolver, error) {
	if typ != dbaas.ProviderTypeClickhouse {
		return nil, xerrors.New("unsupported provider type")
	}
	return r, nil
}

func (r *mockResolver) ResolveHosts() ([]dbaas.ClusterHost, error) {
	return []dbaas.ClusterHost{{Name: "host1", ShardName: "shard1"}, {Name: "host2", ShardName: "shard2"}, {Name: "host3", ShardName: "shard3"}}, nil
}

func (r *mockResolver) ResolveShardGroupHosts(shardGroup string) ([]dbaas.ClusterHost, error) {
	return []dbaas.ClusterHost{{Name: "host1", ShardName: "shard1"}, {Name: "host2", ShardName: "shard2"}}, nil
}

func TestResolveShardGroupHostsAndShards(t *testing.T) {
	oldResolver, _ := dbaas.Current()
	defer func() {
		dbaas.Init(oldResolver)
	}()

	resolver := &mockResolver{}
	dbaas.Init(resolver)

	hosts, shards, err := ResolveShardGroupHostsAndShards("cluster_id", "shardGroup", 9000, 9001)
	require.NoError(t, err)
	require.ElementsMatch(t, hosts, []*clickhouse.Host{
		{Name: "host1", ShardName: "shard1", HTTPPort: 9001, NativePort: 9000},
		{Name: "host2", ShardName: "shard2", HTTPPort: 9001, NativePort: 9000},
	})
	require.Equal(t, shards, map[string][]*clickhouse.Host{
		"shard1": {hosts[0]},
		"shard2": {hosts[1]},
	})

	hosts, shards, err = ResolveShardGroupHostsAndShards("cluster_id", "", 9000, 9001)
	require.NoError(t, err)
	require.ElementsMatch(t, hosts, []*clickhouse.Host{
		{
			Name: "host1", ShardName: "shard1", HTTPPort: 9001, NativePort: 9000,
		},
		{Name: "host2", ShardName: "shard2", HTTPPort: 9001, NativePort: 9000},
		{Name: "host3", ShardName: "shard3", HTTPPort: 9001, NativePort: 9000},
	})
	require.Equal(t, shards, map[string][]*clickhouse.Host{
		"shard1": {hosts[0]},
		"shard2": {hosts[1]},
		"shard3": {hosts[2]},
	})
}
