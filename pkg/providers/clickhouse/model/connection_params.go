package model

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
)

type connectionParams struct {
	ClusterID      string
	User           string
	Password       string
	Database       string
	Secure         bool
	PemFileContent string
	Hosts          []*conn_clickhouse.Host
	Shards         map[string][]*conn_clickhouse.Host
	resolveHosts   func(context.Context, []string) ([]*conn_clickhouse.Host, map[string][]*conn_clickhouse.Host, error)
}

func managedHostsResolver(clusterID, shardGroup string, nativePort, httpPort int) func(context.Context, []string) ([]*conn_clickhouse.Host, map[string][]*conn_clickhouse.Host, error) {
	if clusterID == "" {
		return nil
	}
	return func(ctx context.Context, requiredShards []string) ([]*conn_clickhouse.Host, map[string][]*conn_clickhouse.Host, error) {
		return resolveHostsFromMDBCluster(ctx, clusterID, shardGroup, nativePort, httpPort, requiredShards)
	}
}

// currentHosts preserves the initial shard scope while refreshing the replicas within it.
func (c connectionParams) currentHosts(ctx context.Context) ([]*conn_clickhouse.Host, error) {
	if c.resolveHosts == nil {
		return c.Hosts, nil
	}
	var requiredShards []string
	seen := make(map[string]bool)
	for _, initialHost := range c.Hosts {
		if seen[initialHost.ShardName] {
			continue
		}
		seen[initialHost.ShardName] = true
		requiredShards = append(requiredShards, initialHost.ShardName)
	}
	if len(requiredShards) == 0 {
		return nil, xerrors.New("no ClickHouse hosts configured")
	}
	_, shards, err := c.resolveHosts(ctx, requiredShards)
	if err != nil {
		return nil, xerrors.Errorf("unable to refresh original ClickHouse shards: %w", err)
	}
	var hosts []*conn_clickhouse.Host
	for _, shard := range requiredShards {
		current := shards[shard]
		if len(current) == 0 {
			return nil, xerrors.Errorf("no healthy ClickHouse hosts available for shard %q", shard)
		}
		hosts = append(hosts, current...)
	}
	return hosts, nil
}

func (c connectionParams) SetShards(shards map[string][]*conn_clickhouse.Host) {
	for shardName, hosts := range shards {
		c.Shards[shardName] = append(c.Shards[shardName], hosts...)
	}
}

func ConnectionParamsFromSource(chSource *ChSource, shardGroup string) (*connectionParams, error) {
	var connParams *connectionParams
	if chSource.ConnectionID != "" {
		params, err := ConnectionParamsByConnectionID(chSource.ConnectionID, shardGroup, chSource.NativePort, chSource.HTTPPort)
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve connection params by connection ID: %w", err)
		}
		params.Database = chSource.Database
		connParams = params
	} else {
		connectionHosts, shards, err := resolveHosts(chSource.MdbClusterID, shardGroup, chSource.ShardsList, chSource.NativePort, chSource.HTTPPort)
		if err != nil {
			return nil, err
		}
		secure := chSource.SSLEnabled || chSource.MdbClusterID != ""
		connParams = &connectionParams{
			ClusterID:      chSource.MdbClusterID,
			User:           chSource.User,
			Password:       string(chSource.Password),
			Secure:         secure,
			PemFileContent: chSource.PemFileContent,
			Database:       chSource.Database,
			Hosts:          connectionHosts,
			Shards:         shards,
			resolveHosts:   managedHostsResolver(chSource.MdbClusterID, shardGroup, chSource.NativePort, chSource.HTTPPort),
		}
	}

	return connParams, nil
}

func ConnectionParamsFromDestination(chDestination *ChDestination) (*connectionParams, error) {
	var connParams *connectionParams
	if chDestination.ConnectionID != "" {
		params, err := ConnectionParamsByConnectionID(chDestination.ConnectionID, chDestination.ChClusterName, chDestination.NativePort, chDestination.HTTPPort)
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve connection params by connection ID: %w", err)
		}
		params.Database = chDestination.Database
		connParams = params
	} else {
		connectionHosts, shards, err := resolveHosts(chDestination.MdbClusterID, chDestination.ChClusterName, chDestination.ShardsList, chDestination.NativePort, chDestination.HTTPPort)
		if err != nil {
			return nil, err
		}
		secure := chDestination.SSLEnabled || chDestination.MdbClusterID != ""
		connParams = &connectionParams{
			ClusterID:      chDestination.MdbClusterID,
			User:           chDestination.User,
			Password:       string(chDestination.Password),
			Secure:         secure,
			PemFileContent: chDestination.PemFileContent,
			Database:       chDestination.Database,
			Hosts:          connectionHosts,
			Shards:         shards,
			resolveHosts:   managedHostsResolver(chDestination.MdbClusterID, chDestination.ChClusterName, chDestination.NativePort, chDestination.HTTPPort),
		}
	}

	return connParams, nil
}

func ConnectionParamsByConnectionID(connectionID string, shardGroup string, nativePort int, httpPort int) (*connectionParams, error) {
	if connectionID == "" {
		return nil, xerrors.Errorf("connection ID not filled")
	}

	conn, err := resolveConnection(connectionID)
	if err != nil {
		return nil, xerrors.Errorf("failed to resolve connection: %w", err)
	}

	result := &connectionParams{
		ClusterID:      conn.ClusterID,
		User:           conn.User,
		Password:       string(conn.Password),
		Database:       conn.Database,
		Secure:         conn.HasTLS,
		PemFileContent: conn.CACertificates,
		Hosts:          conn.Hosts,
		Shards:         make(map[string][]*conn_clickhouse.Host),
		resolveHosts:   nil,
	}

	hosts, shards, err := ResolveShardGroupHostsAndShards(context.Background(), conn, connectionID, shardGroup, nativePort, httpPort)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve shard group hosts and shards: %w", err)
	}
	result.Hosts = hosts
	result.Shards = shards

	return result, nil
}
