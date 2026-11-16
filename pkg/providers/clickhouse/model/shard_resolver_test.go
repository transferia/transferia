package model

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/dbaas"
)

// Keep the factory boundary observable: IAM is fetched during construction,
// before the resolver lists hosts, and both stages need one shared budget.
type contextRecordingHostFactory struct {
	mockResolver
	contexts []context.Context
}

var _ dbaas.ResolverFactory = (*contextRecordingHostFactory)(nil)
var _ dbaas.ShardGroupHostsResolver = (*contextRecordingHostFactory)(nil)

func (f *contextRecordingHostFactory) HostResolver(ctx context.Context, _ dbaas.ProviderType, _ string) (dbaas.HostResolver, error) {
	f.contexts = append(f.contexts, ctx)
	return f, nil
}

func (f *contextRecordingHostFactory) ShardGroupHostsResolver(ctx context.Context, _ dbaas.ProviderType, _ string) (dbaas.ShardGroupHostsResolver, error) {
	f.contexts = append(f.contexts, ctx)
	return f, nil
}

func (f *contextRecordingHostFactory) ResolveHostsForShards(ctx context.Context, _ string, _ []string) ([]dbaas.ClusterHost, error) {
	f.contexts = append(f.contexts, ctx)
	return f.ResolveHosts(ctx)
}

func TestMDBDiscoverySharesOneDeadlineAcrossFactoryAndHostRequests(t *testing.T) {
	previous, _ := dbaas.Current()
	t.Cleanup(func() { dbaas.Init(previous) })
	for _, group := range []string{"", "group"} {
		for _, shorterDeadline := range []bool{false, true} {
			factory := new(contextRecordingHostFactory)
			dbaas.Init(factory)
			ctx := context.Background()
			if shorterDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
			}
			before := time.Now()
			_, _, err := resolveHostsFromMDBCluster(ctx, "cluster", group, 9000, 8123, nil)
			after := time.Now()
			require.NoError(t, err)
			require.Len(t, factory.contexts, 2)
			deadline, ok := factory.contexts[0].Deadline()
			require.True(t, ok, "discovery must be bounded even without a caller deadline")
			if shorterDeadline {
				parentDeadline, _ := ctx.Deadline()
				require.Equal(t, parentDeadline, deadline)
			} else {
				require.False(t, deadline.Before(before.Add(time.Minute)))
				require.False(t, deadline.After(after.Add(time.Minute)))
			}
			require.Same(t, factory.contexts[0], factory.contexts[1], "IAM construction and MDB requests share the remaining budget")
			require.ErrorIs(t, factory.contexts[0].Err(), context.Canceled, "completed discovery must release its timeout")
		}
	}
}

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

func (r *mockResolver) HostResolver(_ context.Context, typ dbaas.ProviderType, clusterID string) (dbaas.HostResolver, error) {
	if typ != dbaas.ProviderTypeClickhouse {
		return nil, xerrors.New("unsupported provider type")
	}
	return r, nil
}

func (r *mockResolver) ShardGroupHostsResolver(_ context.Context, typ dbaas.ProviderType, clusterID string) (dbaas.ShardGroupHostsResolver, error) {
	if typ != dbaas.ProviderTypeClickhouse {
		return nil, xerrors.New("unsupported provider type")
	}
	return r, nil
}

func (r *mockResolver) ResolveHosts(context.Context) ([]dbaas.ClusterHost, error) {
	return []dbaas.ClusterHost{{Name: "host1", ShardName: "shard1"}, {Name: "host2", ShardName: "shard2"}, {Name: "host3", ShardName: "shard3"}}, nil
}

func (r *mockResolver) ResolveHostsForShards(ctx context.Context, shardGroup string, _ []string) ([]dbaas.ClusterHost, error) {
	if shardGroup == "" {
		return r.ResolveHosts(ctx)
	}
	return []dbaas.ClusterHost{{Name: "host1", ShardName: "shard1"}, {Name: "host2", ShardName: "shard2"}}, nil
}

func TestResolveHostsFromMDBCluster(t *testing.T) {
	oldResolver, _ := dbaas.Current()
	defer func() {
		dbaas.Init(oldResolver)
	}()

	resolver := &mockResolver{}
	dbaas.Init(resolver)

	hosts, shards, err := resolveHostsFromMDBCluster(context.Background(), "cluster_id", "shardGroup", 9000, 9001, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, hosts, []*clickhouse.Host{
		{Name: "host1", ShardName: "shard1", HTTPPort: 9001, NativePort: 9000},
		{Name: "host2", ShardName: "shard2", HTTPPort: 9001, NativePort: 9000},
	})
	require.Equal(t, shards, map[string][]*clickhouse.Host{
		"shard1": {hosts[0]},
		"shard2": {hosts[1]},
	})

	hosts, shards, err = resolveHostsFromMDBCluster(context.Background(), "cluster_id", "", 9000, 9001, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, hosts, []*clickhouse.Host{{
		Name: "host1", ShardName: "shard1", HTTPPort: 9001, NativePort: 9000},
		{Name: "host2", ShardName: "shard2", HTTPPort: 9001, NativePort: 9000},
		{Name: "host3", ShardName: "shard3", HTTPPort: 9001, NativePort: 9000},
	})
	require.Equal(t, shards, map[string][]*clickhouse.Host{
		"shard1": {hosts[0]},
		"shard2": {hosts[1]},
		"shard3": {hosts[2]},
	})
}

func TestResolveShardGroupHostsAndShards(t *testing.T) {
	oldResolver, _ := dbaas.Current()
	defer func() {
		dbaas.Init(oldResolver)
	}()

	resolver := &mockResolver{}
	dbaas.Init(resolver)

	hosts, shards, err := ResolveShardGroupHostsAndShards(context.Background(), &clickhouse.Connection{
		Hosts: []*clickhouse.Host{
			{Name: "host1", ShardName: "shard1", HTTPPort: 9001, NativePort: 9000},
			{Name: "host2", ShardName: "shard2", HTTPPort: 9001, NativePort: 9000},
			{Name: "host3", ShardName: "shard3", HTTPPort: 9001, NativePort: 9000},
		},
		ShardGroups: map[string][]string{
			"shardGroup": {"shard1", "shard2"},
			"otherGroup": {"shard3"},
		},
	}, "connection_id", "shardGroup", 9000, 9001)
	require.NoError(t, err)

	require.ElementsMatch(t, hosts, []*clickhouse.Host{
		{Name: "host1", ShardName: "shard1", HTTPPort: 9001, NativePort: 9000},
		{Name: "host2", ShardName: "shard2", HTTPPort: 9001, NativePort: 9000},
	})
	require.Equal(t, shards, map[string][]*clickhouse.Host{
		"shard1": {hosts[0]},
		"shard2": {hosts[1]},
	})
}
