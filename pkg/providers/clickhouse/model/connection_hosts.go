package model

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
)

// ConnectionHosts returns a list of hosts which can be used to connect to the ClickHouse cluster with the given shard.
//
// Empty `shard` is supported.
func ConnectionHosts(cfg *ChStorageParams, shard string) ([]string, error) {
	if !cfg.IsManaged() {
		result := connectionHostsOnPremises(cfg, shard)
		if len(result) == 0 {
			return nil, xerrors.Errorf("unable to find hosts for shard: %s", shard)
		}
		return result, nil
	}

	result, err := connectionHostsManaged(cfg, shard)
	if err != nil {
		return nil, xerrors.Errorf("failed to obtain a list of hosts for a managed ClickHouse cluster %q: %w", cfg.MdbClusterID, err)
	}
	return result, nil
}

func connectionHostsOnPremises(cfg *ChStorageParams, shard string) []string {
	if len(cfg.Shards) > 1 && shard != "" {
		return cfg.Shards[shard]
	}
	return cfg.Hosts
}

func connectionHostsManaged(cfg *ChStorageParams, shard string) ([]string, error) {
	shards, err := ShardFromCluster(cfg.MdbClusterID, cfg.ChClusterName)
	if err != nil {
		return nil, xerrors.Errorf("failed to list shards: %w", err)
	}
	if shard == "" {
		for _, v := range shards {
			return v, nil
		}
	}

	result, ok := shards[shard]
	if !ok {
		return nil, xerrors.Errorf("shard %s is absent in the given ClickHouse cluster", shard)
	}
	return result, nil
}
