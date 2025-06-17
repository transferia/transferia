//go:build !disable_clickhouse_provider

package model

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
	"github.com/transferia/transferia/pkg/dbaas"
)

func ShardFromCluster(clusterID, shardGroup string) (map[string][]string, error) {
	hosts, err := resolveShardGroupHosts(clusterID, shardGroup)
	if err != nil {
		return nil, err
	}
	shards := make(map[string][]string)
	for _, h := range hosts {
		if shards[h.ShardName] == nil {
			shards[h.ShardName] = make([]string, 0)
		}
		shards[h.ShardName] = append(shards[h.ShardName], h.Name)
	}

	if len(shards) == 0 {
		return nil, abstract.NewFatalError(xerrors.Errorf("can't find shards for managed ClickHouse '%v'", clusterID))
	}

	logger.Log.Infof("resolved shards: %v", shards)
	return shards, nil
}

func ResolveShardGroupHostsAndShards(clusterID, shardGroup string, nativePort, httpPort int) ([]*clickhouse.Host, map[string][]*clickhouse.Host, error) {
	hosts, err := resolveShardGroupHosts(clusterID, shardGroup)
	if err != nil {
		return nil, nil, err
	}

	shards := make(map[string][]*clickhouse.Host)
	result := make([]*clickhouse.Host, 0, len(hosts))
	for _, host := range hosts {
		connHost := &clickhouse.Host{
			Name:       host.Name,
			NativePort: nativePort,
			HTTPPort:   httpPort,
			ShardName:  host.ShardName,
		}
		shards[host.ShardName] = append(shards[host.ShardName], connHost)
		result = append(result, connHost)
	}

	if len(shards) == 0 {
		return nil, nil, abstract.NewFatalError(xerrors.Errorf("can't find shards for managed ClickHouse '%v'", clusterID))
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

	return result, shards, nil
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
