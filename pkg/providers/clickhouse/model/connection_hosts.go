//go:build !disable_clickhouse_provider

package model

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/connection/clickhouse"
)

// ConnectionHosts returns a list of hosts which can be used to connect to the ClickHouse cluster with the given shard.
//
// Empty `shard` is supported.
func ConnectionHosts(cfg *ChStorageParams, shard string) ([]*clickhouse.Host, error) {
	if !cfg.IsManaged() {
		result := connectionHostsOnPremises(cfg, shard)
		if len(result) == 0 {
			return nil, xerrors.Errorf("unable to find hosts for shard: %s", shard)
		}
		return result, nil
	}

	result, err := connectionHostsManaged(cfg, shard)
	if err != nil {
		return nil, xerrors.Errorf("failed to obtain a list of hosts for a managed ClickHouse cluster %q: %w", cfg.ConnectionParams.ClusterID, err)
	}
	return result, nil
}

func connectionHostsOnPremises(cfg *ChStorageParams, shard string) []*clickhouse.Host {
	if len(cfg.ConnectionParams.Shards) > 1 && shard != "" {
		return cfg.ConnectionParams.Shards[shard]
	}
	return cfg.ConnectionParams.Hosts
}

func connectionHostsManaged(cfg *ChStorageParams, shard string) ([]*clickhouse.Host, error) {
	if shard == "" {
		for _, v := range cfg.ConnectionParams.Shards {
			return v, nil
		}
	}

	result, ok := cfg.ConnectionParams.Shards[shard]
	if !ok {
		return nil, xerrors.Errorf("shard %s is absent in the given ClickHouse cluster", shard)
	}
	return result, nil
}
