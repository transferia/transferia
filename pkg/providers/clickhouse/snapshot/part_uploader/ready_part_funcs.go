package part_uploader

import (
	"context"
	"slices"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"golang.org/x/sync/errgroup"
)

// A ready name is evidence that ALL shards passed INSERT acknowledgement and
// row-count validation before the first rename. Reassignment can finish a
// partially acknowledged rename sequence, but must never insert into that part.
func restoreReady(ctx context.Context, client *ch_eo_common.Client, operations TableOperations, tableState *ch_eo_common.SnapshotTable, attemptKey string, completePendingRenames bool) (*ch_eo_common.ReadyPart, error) {
	nodesByShard := client.NodesByShard()
	loadingTableName := tableState.TemporaryTablePrefix + attemptKey + "_loading"
	readyTableName := tableState.TemporaryTablePrefix + attemptKey + "_ready"
	loadingUUIDsByShard := make([]string, len(nodesByShard))
	readyUUIDsByShard := make([]string, len(nodesByShard))
	var inspections errgroup.Group
	inspections.SetLimit(32)
	for shardIndex, shardNodes := range nodesByShard {
		inspections.Go(func() error {
			hostName := shardNodes[0].Host.Name
			var err error
			loadingUUIDsByShard[shardIndex], err = client.TableUUID(ctx, hostName, loadingTableName)
			if err != nil {
				return xerrors.Errorf("inspect loading name: %w", err)
			}
			readyUUIDsByShard[shardIndex], err = client.TableUUID(ctx, hostName, readyTableName)
			if err != nil {
				return xerrors.Errorf("inspect ready name: %w", err)
			}
			return nil
		})
	}
	if err := inspections.Wait(); err != nil {
		return nil, xerrors.Errorf("inspect snapshot shards: %w", err)
	}
	hasReadyTable := false
	for shardIndex, shardNodes := range nodesByShard {
		hostName := shardNodes[0].Host.Name
		identityHostName := operations.PartTableIdentityHostName(hostName)
		expectedTableUUID := ch_eo_common.PartTableUUID(tableState.TemporaryTablePrefix, attemptKey, identityHostName)
		for _, actualTableUUID := range []string{loadingUUIDsByShard[shardIndex], readyUUIDsByShard[shardIndex]} {
			if actualTableUUID != "" && actualTableUUID != expectedTableUUID {
				return nil, xerrors.New("part table UUID changed")
			}
		}
		if loadingUUIDsByShard[shardIndex] != "" && readyUUIDsByShard[shardIndex] != "" {
			return nil, xerrors.New("part has both loading and ready tables")
		}
		hasReadyTable = hasReadyTable || readyUUIDsByShard[shardIndex] != ""
	}
	if !hasReadyTable {
		return nil, nil
	}
	missingReadyTables := make([]*ch_eo_common.OwnedTable, 0)
	seenTableUUIDs := make(map[string]bool)
	for shardIndex, shardNodes := range nodesByShard {
		if readyUUIDsByShard[shardIndex] != "" {
			continue
		}
		if loadingUUIDsByShard[shardIndex] == "" {
			return nil, xerrors.New("verified part is missing on a shard")
		}
		if !completePendingRenames {
			return nil, xerrors.New("part publication is incomplete after the loader barrier")
		}
		if !seenTableUUIDs[loadingUUIDsByShard[shardIndex]] {
			seenTableUUIDs[loadingUUIDsByShard[shardIndex]] = true
			missingReadyTables = append(missingReadyTables, ch_eo_common.NewOwnedTable(shardNodes[0].Host.Name, loadingTableName, loadingUUIDsByShard[shardIndex]))
		}
	}
	var renames errgroup.Group
	renames.SetLimit(32)
	for _, table := range missingReadyTables {
		renames.Go(func() error {
			if err := operations.RenamePartTable(ctx, table, readyTableName); err != nil {
				return xerrors.Errorf("finish ready part rename: %w", err)
			}
			return nil
		})
	}
	if err := renames.Wait(); err != nil {
		return nil, xerrors.Errorf("finish ready shard renames: %w", err)
	}

	readyTables := make([]ch_eo_common.OwnedTable, len(nodesByShard))
	readyRowCounts := make([]uint64, len(nodesByShard))
	var readyChecks errgroup.Group
	readyChecks.SetLimit(32)
	for shardIndex, shardNodes := range nodesByShard {
		readyChecks.Go(func() error {
			hostName := shardNodes[0].Host.Name
			identityHostName := operations.PartTableIdentityHostName(hostName)
			expectedTableUUID := ch_eo_common.PartTableUUID(tableState.TemporaryTablePrefix, attemptKey, identityHostName)
			actualTableUUID, err := client.TableUUID(ctx, hostName, readyTableName)
			if err != nil {
				return xerrors.Errorf("check ready UUID: %w", err)
			}
			if actualTableUUID != expectedTableUUID {
				return xerrors.New("ready part UUID mismatch")
			}
			readyRowCounts[shardIndex], err = client.CountPhysicalRows(ctx, hostName, readyTableName)
			if err != nil {
				return xerrors.Errorf("count ready part: %w", err)
			}
			readyTables[shardIndex] = *ch_eo_common.NewOwnedTable(identityHostName, readyTableName, expectedTableUUID)
			return nil
		})
	}
	if err := readyChecks.Wait(); err != nil {
		return nil, xerrors.Errorf("read ready snapshot shards: %w", err)
	}
	readyPart := ch_eo_common.NewReadyPart()
	for shardIndex := range nodesByShard {
		readyPart.AddShard(readyTables[shardIndex], readyRowCounts[shardIndex])
	}
	return readyPart, nil
}

// discoverReadyAttempts groups table names by logical part. Prefer an attempt
// ready on every shard, then use a stable order if several attempts completed.
// An abandoned loading attempt must not hide another attempt's verified rows.
func discoverReadyAttempts(ctx context.Context, client *ch_eo_common.Client, databaseName string, tablePrefix string, namePrefix string) (map[string][]string, error) {
	tableNamesByShard := make([][]string, len(client.NodesByShard()))
	var discoveries errgroup.Group
	discoveries.SetLimit(32)
	for shardIndex, shardNodes := range client.NodesByShard() {
		discoveries.Go(func() error {
			var err error
			tableNamesByShard[shardIndex], err = client.ReadStrings(ctx, shardNodes[0].Host.Name, "SELECT name FROM system.tables WHERE database = ? AND startsWith(name, ?)", databaseName, namePrefix)
			if err != nil {
				return xerrors.Errorf("discover snapshot parts: %w", err)
			}
			return nil
		})
	}
	if err := discoveries.Wait(); err != nil {
		return nil, xerrors.Errorf("discover snapshot shards: %w", err)
	}
	attemptsByPart := make(map[string][]string)
	readyShardCounts := make(map[string]int)
	for _, tableNames := range tableNamesByShard {
		for _, tableName := range tableNames {
			attemptKey, isPartTable := ch_eo_common.PartAttemptKeyFromTableName(tablePrefix, tableName)
			if !isPartTable {
				continue
			}
			partIDHash := attemptKey[:32]
			if _, found := attemptsByPart[partIDHash]; !found {
				attemptsByPart[partIDHash] = nil
			}
			if !strings.HasSuffix(tableName, "_ready") {
				continue
			}
			if readyShardCounts[attemptKey] == 0 {
				attemptsByPart[partIDHash] = append(attemptsByPart[partIDHash], attemptKey)
			}
			readyShardCounts[attemptKey]++
		}
	}
	for _, attempts := range attemptsByPart {
		slices.SortFunc(attempts, func(first string, second string) int {
			if readyShardCounts[first] != readyShardCounts[second] {
				return readyShardCounts[second] - readyShardCounts[first]
			}
			return strings.Compare(first, second)
		})
	}
	return attemptsByPart, nil
}

func DiscoverReady(ctx context.Context, client *ch_eo_common.Client, operations TableOperations, databaseName string, tableState *ch_eo_common.SnapshotTable) ([]ch_eo_common.ReadyPart, error) {
	attemptsByPart, err := discoverReadyAttempts(ctx, client, databaseName, tableState.TemporaryTablePrefix, tableState.TemporaryTablePrefix)
	if err != nil {
		return nil, xerrors.Errorf("discover ready parts: %w", err)
	}
	var readyParts []ch_eo_common.ReadyPart
	for _, attempts := range attemptsByPart {
		if len(attempts) == 0 {
			return nil, xerrors.New("no verified attempt for a discovered loading part")
		}
		readyPart, err := restoreReady(ctx, client, operations, tableState, attempts[0], false)
		if err != nil {
			return nil, xerrors.Errorf("read discovered part: %w", err)
		}
		if readyPart == nil {
			return nil, xerrors.New("verified part disappeared after the loader barrier")
		}
		readyParts = append(readyParts, *readyPart)
	}
	return readyParts, nil
}
