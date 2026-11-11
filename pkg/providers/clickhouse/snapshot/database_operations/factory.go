package database_operations

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/errgroup"
)

// New selects and validates a mode before any snapshot table is created.
func New(ctx context.Context, logger log.Logger, transferID string, databaseName string, cleanupPolicy model.CleanupType, tablesClient *ch_eo_common.Client) (DatabaseOperations, error) {
	availableNodesByShard, err := tablesClient.SelectReplicas(ctx, true)
	if err != nil {
		return nil, xerrors.Errorf("discover database nodes: %w", err)
	}
	enginesByShard := make([][]string, len(availableNodesByShard))
	var engineChecks errgroup.Group
	engineChecks.SetLimit(32)
	for shardIndex, shardNodes := range availableNodesByShard {
		enginesByShard[shardIndex] = make([]string, len(shardNodes))
		for nodeIndex, node := range shardNodes {
			engineChecks.Go(func() error {
				var engine string
				if err := tablesClient.ReadRow(ctx, node.Host.Name, "SELECT engine FROM system.databases WHERE name = ?", []any{databaseName}, &engine); err != nil {
					return xerrors.Errorf("read snapshot database engine: %w", err)
				}
				if engine != "Atomic" && engine != "Replicated" {
					return xerrors.Errorf("snapshot requires an Atomic or Replicated database, found %s", engine)
				}
				enginesByShard[shardIndex][nodeIndex] = engine
				return nil
			})
		}
	}
	if err := engineChecks.Wait(); err != nil {
		return nil, xerrors.Errorf("check snapshot database engines: %w", err)
	}
	databaseEngine := ""
	for _, shardEngines := range enginesByShard {
		for _, engine := range shardEngines {
			if databaseEngine != "" && databaseEngine != engine {
				return nil, xerrors.New("snapshot database engines differ between hosts")
			}
			databaseEngine = engine
		}
	}
	switch databaseEngine {
	case "Atomic":
		databaseOperations := newAtomic(logger, transferID, databaseName, cleanupPolicy, tablesClient, nil, nil)
		if err := databaseOperations.validateTopology(availableNodesByShard); err != nil {
			return nil, xerrors.Errorf("validate Atomic snapshot topology: %w", err)
		}
		return databaseOperations, nil
	case "Replicated":
		databaseOperations := newReplicated(logger, transferID, databaseName, cleanupPolicy, tablesClient, nil, nil)
		if err := databaseOperations.validateTopology(ctx, availableNodesByShard); err != nil {
			return nil, xerrors.Errorf("validate Replicated snapshot topology: %w", err)
		}
		return databaseOperations, nil
	default:
		return nil, xerrors.New("snapshot database has no available nodes")
	}
}
