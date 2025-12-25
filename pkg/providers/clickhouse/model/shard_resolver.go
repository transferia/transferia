package model

import (
	"slices"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/dbaas"
)

func ResolveHostsFromMDBCluster(clusterID string, shardGroup string, nativePort, httpPort int) ([]*clickhouse.Host, map[string][]*clickhouse.Host, error) {
	hosts, err := resolveShardGroupHosts(clusterID, shardGroup)
	if err != nil {
		return nil, nil, err
	}

	shards := make(map[string][]*clickhouse.Host)
	resultHosts := make([]*clickhouse.Host, 0, len(hosts))
	for _, host := range hosts {
		connHost := &clickhouse.Host{
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

func ResolveShardGroupHostsAndShards(clickhouseConn *clickhouse.Connection, connectionID, shardGroup string, nativePort, httpPort int) ([]*clickhouse.Host, map[string][]*clickhouse.Host, error) {
	if connectionID != "" {
		hosts, err := resolveHostsByConnection(clickhouseConn, shardGroup)
		if err != nil {
			return nil, nil, xerrors.Errorf("unable to resolve hosts by connection %s: %w", connectionID, err)
		}
		return hosts, resolveShardsByHosts(hosts), nil
	}

	return ResolveHostsFromMDBCluster(clickhouseConn.ClusterID, shardGroup, nativePort, httpPort)
}

func resolveHostsByConnection(clickhouseConn *clickhouse.Connection, shardGroup string) ([]*clickhouse.Host, error) {
	hosts := make([]*clickhouse.Host, 0)
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

func resolveShardsByHosts(hosts []*clickhouse.Host) map[string][]*clickhouse.Host {
	shards := make(map[string][]*clickhouse.Host)
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

func resolveShardGroupHosts(clusterID, shardGroup string) ([]dbaas.ClusterHost, error) {
	var hosts []dbaas.ClusterHost
	if shardGroup != "" {
		dbaasResolver, err := dbaas.Current()
		if err != nil {
			return nil, xerrors.Errorf("unable to get dbaas resolver: %w", err)
		}
		shardGroupResolver, err := dbaasResolver.ShardGroupHostsResolver(dbaas.ProviderTypeClickhouse, clusterID)
		if err != nil {
			return nil, xerrors.Errorf("unable to get shard group resolver: %w", err)
		}
		hosts, err = shardGroupResolver.ResolveShardGroupHosts(shardGroup)
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve shard group hosts: %w", err)
		}
	} else {
		var err error
		hosts, err = dbaas.ResolveClusterHosts(dbaas.ProviderTypeClickhouse, clusterID)
		if err != nil {
			return nil, xerrors.Errorf("unable to list hosts: %w", err)
		}
	}

	return hosts, nil
}
