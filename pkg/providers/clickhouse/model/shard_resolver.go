package model

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/dbaas"
)

func ShardFromCluster(clusterID, shardGroup string) (map[string][]string, error) {
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
	shards := make(map[string][]string)
	for _, h := range hosts {
		if shards[h.ShardName] == nil {
			shards[h.ShardName] = make([]string, 0)
		}
		shards[h.ShardName] = append(shards[h.ShardName], h.Name)
	}
	logger.Log.Infof("resolved shards: %v", shards)
	return shards, nil
}
