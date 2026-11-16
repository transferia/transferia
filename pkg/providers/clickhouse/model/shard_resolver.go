package model

import (
	"context"
	"slices"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/dbaas"
)

func resolveHostsFromMDBCluster(ctx context.Context, clusterID, shardGroup string, nativePort, httpPort int, requiredShards []string) ([]*conn_clickhouse.Host, map[string][]*conn_clickhouse.Host, error) {
	// Bound the whole discovery operation, including IAM and retry delays,
	// even when the caller has no deadline.
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	hosts, err := resolveShardGroupHosts(ctx, clusterID, shardGroup, requiredShards)
	if err != nil {
		return nil, nil, err
	}

	shards := make(map[string][]*conn_clickhouse.Host)
	resultHosts := make([]*conn_clickhouse.Host, 0, len(hosts))
	for _, host := range hosts {
		connHost := &conn_clickhouse.Host{
			Name:       host.Name,
			NativePort: nativePort,
			HTTPPort:   httpPort,
			ShardName:  host.ShardName,
		}
		shards[host.ShardName] = append(shards[host.ShardName], connHost)
		resultHosts = append(resultHosts, connHost)
	}

	if len(shards) == 0 {
		return nil, nil, abstract.NewFatalError(xerrors.Errorf("can't find shards for managed ClickHouse '%v'", clusterID))
	}

	return resultHosts, resolveShardsByHosts(resultHosts), nil
}

func ResolveShardGroupHostsAndShards(ctx context.Context, clickhouseConn *conn_clickhouse.Connection, connectionID, shardGroup string, nativePort, httpPort int) ([]*conn_clickhouse.Host, map[string][]*conn_clickhouse.Host, error) {
	if connectionID != "" {
		hosts, err := resolveHostsByConnection(clickhouseConn, shardGroup)
		if err != nil {
			return nil, nil, xerrors.Errorf("unable to resolve hosts by connection %s: %w", connectionID, err)
		}
		return hosts, resolveShardsByHosts(hosts), nil
	}

	return resolveHostsFromMDBCluster(ctx, clickhouseConn.ClusterID, shardGroup, nativePort, httpPort, nil)
}

func resolveHostsByConnection(clickhouseConn *conn_clickhouse.Connection, shardGroup string) ([]*conn_clickhouse.Host, error) {
	hosts := make([]*conn_clickhouse.Host, 0)
	if shardGroup == "" {
		return clickhouseConn.Hosts, nil
	}
	if shards, ok := clickhouseConn.ShardGroups[shardGroup]; ok {
		for _, host := range clickhouseConn.Hosts {
			if slices.Contains(shards, host.ShardName) {
				hosts = append(hosts, host)
			}
		}
	}

	if len(hosts) == 0 {
		return nil, abstract.NewFatalError(xerrors.Errorf("can't find shards for connection of ClickHouse '%v' and shard group '%v'", clickhouseConn, shardGroup))
	}

	return hosts, nil
}

func resolveShardsByHosts(hosts []*conn_clickhouse.Host) map[string][]*conn_clickhouse.Host {
	shards := make(map[string][]*conn_clickhouse.Host)
	for _, host := range hosts {
		shards[host.ShardName] = append(shards[host.ShardName], host)
	}

	// shardHostsMap is only for logging
	shardHostsMap := make(map[string][]string)
	for shardName, hosts := range shards {
		shardHostsMap[shardName] = make([]string, 0, len(hosts))
		for _, host := range hosts {
			shardHostsMap[shardName] = append(shardHostsMap[shardName], host.Name)
		}
	}
	logger.Log.Infof("resolved shards: %v", shardHostsMap)
	return shards
}

func resolveShardGroupHosts(ctx context.Context, clusterID, shardGroup string, requiredShards []string) ([]dbaas.ClusterHost, error) {
	if err := ctx.Err(); err != nil {
		return nil, xerrors.Errorf("ClickHouse discovery canceled: %w", err)
	}
	dbaasResolver, err := dbaas.Current()
	if err != nil {
		return nil, xerrors.Errorf("unable to get dbaas resolver: %w", err)
	}
	resolver, err := dbaasResolver.ShardGroupHostsResolver(ctx, dbaas.ProviderTypeClickhouse, clusterID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get shard group resolver: %w", err)
	}
	hosts, err := resolver.ResolveHostsForShards(ctx, shardGroup, requiredShards)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve shard group hosts: %w", err)
	}
	return hosts, nil
}
