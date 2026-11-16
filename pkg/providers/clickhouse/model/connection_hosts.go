package model

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	conn_clickhouse "github.com/transferia/transferia/pkg/connection/clickhouse"
)

// ConnectionHosts cancels managed discovery along with the operation
// opening a new connection. Static and ConnMan configurations retain their hosts.
func ConnectionHosts(ctx context.Context, cfg *ChStorageParams, shard string) ([]*conn_clickhouse.Host, error) {
	if err := ctx.Err(); err != nil {
		return nil, xerrors.Errorf("ClickHouse host resolution canceled: %w", err)
	}
	if cfg.ConnectionParams.resolveHosts != nil {
		if shard == "" {
			found := false
			for initial := range cfg.ConnectionParams.Shards {
				if !found || initial < shard {
					shard, found = initial, true
				}
			}
			if !found {
				return nil, xerrors.New("no ClickHouse shards configured")
			}
		}
		if _, ok := cfg.ConnectionParams.Shards[shard]; !ok {
			return nil, xerrors.Errorf("ClickHouse shard %q is not present in the initial configuration", shard)
		}
		_, shards, err := cfg.ConnectionParams.resolveHosts(ctx, []string{shard})
		if err != nil {
			return nil, xerrors.Errorf("failed to refresh ClickHouse hosts: %w", err)
		}
		hosts := shards[shard]
		if len(hosts) == 0 {
			return nil, xerrors.Errorf("no healthy ClickHouse hosts available for shard %q", shard)
		}
		return hosts, nil
	}
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

func connectionHostsOnPremises(cfg *ChStorageParams, shard string) []*conn_clickhouse.Host {
	if len(cfg.ConnectionParams.Shards) > 1 && shard != "" {
		return cfg.ConnectionParams.Shards[shard]
	}
	return cfg.ConnectionParams.Hosts
}

func connectionHostsManaged(cfg *ChStorageParams, shard string) ([]*conn_clickhouse.Host, error) {
	if shard == "" {
		for _, v := range cfg.ConnectionParams.Shards {
			return v, nil
		}
	}

	result, ok := cfg.ConnectionParams.Shards[shard]
	if !ok {
		return nil, xerrors.Errorf("shard %s is absent in the given ClickHouse cluster", shard)
	}
	if len(result) == 0 {
		return nil, xerrors.Errorf("no hosts available for ClickHouse shard %q", shard)
	}
	return result, nil
}
