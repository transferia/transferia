package model

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/connection"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/dbaas"
)

// A mutable discovery snapshot isolates address refresh and shard routing from
// MDB's health filter, which is exercised with the real resolver in ycdbaas tests.
type changingHostsResolver struct {
	dbaas.ResolverFactory
	hosts []dbaas.ClusterHost
	group string
}

var _ dbaas.ResolverFactory = (*changingHostsResolver)(nil)
var _ dbaas.HostResolver = (*changingHostsResolver)(nil)
var _ dbaas.ShardGroupHostsResolver = (*changingHostsResolver)(nil)

func (r *changingHostsResolver) HostResolver(context.Context, dbaas.ProviderType, string) (dbaas.HostResolver, error) {
	return r, nil
}

func (r *changingHostsResolver) ResolveHosts(context.Context) ([]dbaas.ClusterHost, error) {
	return r.hosts, nil
}

func (r *changingHostsResolver) ShardGroupHostsResolver(context.Context, dbaas.ProviderType, string) (dbaas.ShardGroupHostsResolver, error) {
	return r, nil
}

func (r *changingHostsResolver) ResolveHostsForShards(_ context.Context, group string, _ []string) ([]dbaas.ClusterHost, error) {
	r.group = group
	return r.hosts, nil
}

// Reconnection must use new replica addresses while retaining the planned shard,
// shard group, ports, and the immutable snapshot used to build the transfer.
func TestConnectionHostsRefreshesReplicasWithoutChangingOriginalTopology(t *testing.T) {
	previous, _ := dbaas.Current()
	t.Cleanup(func() { dbaas.Init(previous) })
	resolver := &changingHostsResolver{hosts: []dbaas.ClusterHost{{Name: "before", ShardName: "shard", Health: dbaas.ALIVE}}}
	dbaas.Init(resolver)
	source := &ChSource{MdbClusterID: "cluster", ChClusterName: "group", NativePort: 9440, HTTPPort: 8443}
	params, err := source.ToStorageParams()
	require.NoError(t, err)
	// MDB replaces the replica after the initial configuration has been captured.
	resolver.hosts = []dbaas.ClusterHost{{Name: "after", ShardName: "shard", Health: dbaas.ALIVE}}

	hosts, err := ConnectionHosts(context.Background(), params, "shard")
	require.NoError(t, err)
	require.Equal(t, []*conn_clickhouse.Host{{Name: "after", ShardName: "shard", NativePort: 9440, HTTPPort: 8443}}, hosts)
	require.Equal(t, "before", params.ConnectionParams.Hosts[0].Name, "refresh must not mutate the initial topology")
	require.Equal(t, "group", resolver.group)
	// "aaa" sorts before the original shard. It must not become the default,
	// and an explicit request must not adopt a shard absent from the initial plan.
	resolver.hosts = append(resolver.hosts, dbaas.ClusterHost{Name: "new-shard", ShardName: "aaa", Health: dbaas.ALIVE})
	hosts, err = ConnectionHosts(context.Background(), params, "")
	require.NoError(t, err)
	require.Equal(t, "shard", hosts[0].ShardName, "new shards must not replace the initial source shard")
	_, err = ConnectionHosts(context.Background(), params, "aaa")
	require.ErrorContains(t, err, "initial configuration")
	// A healthy sibling cannot stand in for the original shard when it disappears.
	resolver.hosts = []dbaas.ClusterHost{{Name: "other-shard", ShardName: "other", Health: dbaas.ALIVE}}
	_, err = ConnectionHosts(context.Background(), params, "shard")
	require.ErrorContains(t, err, "shard")
}

// Source and destination wrappers share the sink-parameter contract. A child
// created for one shard may refresh its replicas, but cannot expand to siblings.
func TestSinkResolveHostsRefreshesOnlyTheChildShard(t *testing.T) {
	previous, _ := dbaas.Current()
	t.Cleanup(func() { dbaas.Init(previous) })
	for _, destination := range []bool{false, true} {
		resolver := &changingHostsResolver{hosts: []dbaas.ClusterHost{
			{Name: "first-before", ShardName: "first", Health: dbaas.ALIVE},
			{Name: "second-before", ShardName: "second", Health: dbaas.ALIVE},
		}}
		dbaas.Init(resolver)
		var wrapper ChSinkParams
		if destination {
			params, err := ConnectionParamsFromDestination(&ChDestination{MdbClusterID: "cluster", NativePort: 9000, HTTPPort: 8123})
			require.NoError(t, err)
			wrapper = ChDestinationWrapper{Model: new(ChDestination), connectionParams: *params}
		} else {
			params, err := ConnectionParamsFromSource(&ChSource{MdbClusterID: "cluster", NativePort: 9000, HTTPPort: 8123}, "")
			require.NoError(t, err)
			wrapper = ChSourceWrapper{Model: new(ChSource), connectionParams: *params}
		}
		child := wrapper.MakeChildShardParams(wrapper.Shards()["first"])
		// Both existing shards get new replicas and a third shard appears.
		// This child still owns only the first shard selected during planning.
		resolver.hosts = []dbaas.ClusterHost{
			{Name: "first-after", ShardName: "first", Health: dbaas.ALIVE},
			{Name: "second-after", ShardName: "second", Health: dbaas.ALIVE},
			{Name: "new-shard", ShardName: "third", Health: dbaas.ALIVE},
		}
		hosts, err := child.ResolveHosts(context.Background())
		require.NoError(t, err)
		require.Equal(t, []*conn_clickhouse.Host{{Name: "first-after", ShardName: "first", NativePort: 9000, HTTPPort: 8123}}, hosts)
		require.Equal(t, "first-before", child.AltHosts()[0].Name)
		// If that shard disappears, fail instead of borrowing the healthy sibling.
		resolver.hosts = resolver.hosts[1:]
		_, err = child.ResolveHosts(context.Background())
		require.ErrorContains(t, err, "first")
	}
}

// A ConnMan connection can carry a managed cluster ID, but its explicit hosts
// and ports remain authoritative over the endpoint defaults and raw MDB discovery.
func TestConnectionHostsUsesConnManHostsInsteadOfEndpointDefaults(t *testing.T) {
	previous := connection.Resolver()
	t.Cleanup(func() { connection.Init(previous) })
	resolver := connection.NewStubConnectionResolver()
	configured := &conn_clickhouse.Host{Name: "connman", ShardName: "shard", NativePort: 9441, HTTPPort: 8444}
	require.NoError(t, resolver.Add("connection", &conn_clickhouse.Connection{ClusterID: "cluster", Hosts: []*conn_clickhouse.Host{configured}}))
	connection.Init(resolver)
	params, err := (&ChSource{ConnectionID: "connection", NativePort: 9000, HTTPPort: 8123}).ToStorageParams()
	require.NoError(t, err)
	hosts, err := ConnectionHosts(context.Background(), params, "shard")
	require.NoError(t, err)
	require.Equal(t, []*conn_clickhouse.Host{configured}, hosts)
}

// A cluster ID alone does not enable dynamic discovery. Configurations without
// a refresh callback must keep their supplied hosts, including ConnMan-backed ones.
func TestConnectionHostsKeepsStaticHostsWhenNoRefreshCallbackExists(t *testing.T) {
	initial := &conn_clickhouse.Host{Name: "configured", ShardName: "shard", NativePort: 9000, HTTPPort: 8123}
	for _, clusterID := range []string{"", "connman-cluster"} {
		t.Run(clusterID, func(t *testing.T) {
			params := &ChStorageParams{ConnectionParams: &connectionParams{
				ClusterID: clusterID,
				Hosts:     []*conn_clickhouse.Host{initial},
				Shards:    map[string][]*conn_clickhouse.Host{"shard": {initial}},
			}}
			hosts, err := ConnectionHosts(context.Background(), params, "shard")
			require.NoError(t, err)
			require.Equal(t, []*conn_clickhouse.Host{initial}, hosts)
		})
	}
}

func TestConnectionHosts(t *testing.T) {
	// DTSUPPORT-5356
	srcModel := ChSource{
		MdbClusterID: "",
		ShardsList:   nil,
	}
	storageParams, err := srcModel.ToStorageParams()
	require.NoError(t, err)
	result, err := ConnectionHosts(context.Background(), storageParams, "")
	require.Error(t, err)
	require.Equal(t, len(result), 0)
}
