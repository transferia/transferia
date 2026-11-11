package database_operations

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

// No rows have been copied into this private table yet. Its merge lock protects
// the physical row-count check, which must finish before publication is decided.
// A server restart loses the lock; an unverified generation must then be rebuilt.
func (s *tableCommands) stopPrivateTableMerges(ctx context.Context, table *ch_eo_common.OwnedTable) error {
	return s.executeOwnedTableCommand(ctx, table, "SYSTEM STOP MERGES "+s.tablesClient.QualifiedTableName(table.TableName))
}

// Release the candidate before returning verified build results. Therefore any
// durably recorded candidate is both complete and free of our merge lock, even
// if a replica becomes unavailable immediately before publication.
func (s *tableCommands) startPrivateTableMerges(ctx context.Context, table *ch_eo_common.OwnedTable) error {
	return s.executeOwnedTableCommand(ctx, table, "SYSTEM START MERGES "+s.tablesClient.QualifiedTableName(table.TableName))
}

func (d *replicatedOperations) stopPrivateTableMerges(ctx context.Context, table *ch_eo_common.OwnedTable) ([]string, error) {
	nodesByShard, err := d.SelectNodesByShard(ctx)
	if err != nil {
		return nil, xerrors.Errorf("select replicas before stopping merges: %w", err)
	}
	var stoppedHosts []string
	// Every replica shares the UUID used as the pending-command key, so admit
	// these commands one at a time. Data is copied only after the last STOP.
	for _, shardNodes := range nodesByShard {
		for _, node := range shardNodes {
			if err := d.SyncDatabaseDDLOnNode(ctx, node.Host.Name); err != nil {
				return nil, xerrors.Errorf("wait for private table before stopping merges: %w", err)
			}
			localTable := *table
			localTable.CommandHostName = node.Host.Name
			if err := d.tableCommands.stopPrivateTableMerges(ctx, &localTable); err != nil {
				return nil, xerrors.Errorf("stop private table merges on %s: %w", node.Host.Name, err)
			}
			stoppedHosts = append(stoppedHosts, node.Host.Name)
		}
	}
	return stoppedHosts, nil
}

func (d *replicatedOperations) startPrivateTableMerges(ctx context.Context, table *ch_eo_common.OwnedTable, stoppedHosts []string) error {
	// Do not reselect available replicas: every host that received STOP must
	// acknowledge START before this shared candidate can be published.
	for _, hostName := range stoppedHosts {
		localTable := *table
		localTable.CommandHostName = hostName
		if err := d.tableCommands.startPrivateTableMerges(ctx, &localTable); err != nil {
			return xerrors.Errorf("restart private table merges on %s: %w", hostName, err)
		}
	}
	return nil
}
