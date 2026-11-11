package database_operations

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/errgroup"
)

type replicatedOperations struct {
	transferID    string
	cleanupPolicy model.CleanupType
	*tableCommands
}

var _ DatabaseOperations = (*replicatedOperations)(nil)

func newReplicated(logger log.Logger, transferID string, databaseName string, cleanupPolicy model.CleanupType, tablesClient *ch_eo_common.Client, table *ch_eo_common.SnapshotTable, checkpoint CommandCheckpoint) *replicatedOperations {
	databaseOperations := &replicatedOperations{
		transferID:    transferID,
		cleanupPolicy: cleanupPolicy,
		tableCommands: nil,
	}
	databaseOperations.tableCommands = newTableCommands(logger, databaseName, tablesClient, table, databaseOperations, checkpoint)
	return databaseOperations
}

// Replicated databases execute ATTACH PARTITION ... FROM across the database.
// Assemble each input exactly once, then import each assembled partition once.
// Repeating these commands per shard would duplicate already attached rows.
func (d *replicatedOperations) buildSharedCandidateTableOnce(ctx context.Context, tableState *ch_eo_common.SnapshotTable, readyParts []ch_eo_common.ReadyPart, cleanupTables *[]*ch_eo_common.OwnedTable) ([]ch_eo_common.NodeState, error) {
	_, assemblyEngine, _, err := ch_eo_common.ExtractTableDefinitionParts(tableState.DDLTemplate)
	if err != nil {
		return nil, xerrors.Errorf("buildSharedCandidateTableOnce: %w", err)
	}
	assemblyEngine, err = ch_eo_common.MakeEngineNonReplicated(assemblyEngine)
	if err != nil {
		return nil, xerrors.Errorf("buildSharedCandidateTableOnce: %w", err)
	}
	assemblyTable := ch_eo_common.NewTemporaryTable(tableState.TemporaryTablePrefix, d.tableCommands.tablesClient.NodesByShard()[0][0].Host.Name, ch_eo_common.RoleAssembly)
	*cleanupTables = append(*cleanupTables, assemblyTable)
	assemblyDDL, err := ch_eo_common.BuildCreateTableDDL(d.tableCommands.databaseName, assemblyTable.TableName, assemblyTable.ExpectedTableUUID, tableState.DDLTemplate, assemblyEngine)
	if err != nil {
		return nil, xerrors.Errorf("buildSharedCandidateTableOnce: %w", err)
	}
	if err := d.tableCommands.executeOwnedTableCommand(ctx, assemblyTable, assemblyDDL); err != nil {
		return nil, xerrors.Errorf("create shared assembly: %w", err)
	}
	if _, err := d.stopPrivateTableMerges(ctx, assemblyTable); err != nil {
		return nil, xerrors.Errorf("stop shared assembly merges: %w", err)
	}
	expectedRowCountsByShard := make([]uint64, len(d.tableCommands.tablesClient.NodesByShard()))
	if d.cleanupPolicy == model.DisabledCleanup && tableState.NodeStates[0].PreviousTableUUID != "" {
		availableNodesByShard, err := d.tableCommands.tablesClient.SelectReplicas(ctx, true)
		if err != nil {
			return nil, xerrors.Errorf("buildSharedCandidateTableOnce: %w", err)
		}
		partNamesBeforeAttachByHost := make(map[string]string)
		for _, shardNodes := range availableNodesByShard {
			for _, node := range shardNodes {
				partNamesBeforeAttachByHost[node.Host.Name], err = d.tableCommands.tablesClient.JoinedActivePartNames(ctx, node.Host.Name, tableState.DestinationTableName)
				if err != nil {
					return nil, xerrors.Errorf("capture old table: %w", err)
				}
			}
		}
		for shardIndex, shardNodes := range d.tableCommands.tablesClient.NodesByShard() {
			expectedRowCountsByShard[shardIndex], err = d.tableCommands.tablesClient.CountPhysicalRows(ctx, shardNodes[0].Host.Name, tableState.DestinationTableName)
			if err != nil {
				return nil, xerrors.Errorf("count old table: %w", err)
			}
		}
		if err := d.attachSharedTablePartitions(ctx, assemblyTable, tableState.DestinationTableName); err != nil {
			return nil, xerrors.Errorf("capture old table: %w", err)
		}
		for hostName, partNamesBeforeAttach := range partNamesBeforeAttachByHost {
			partNamesAfterAttach, err := d.tableCommands.tablesClient.JoinedActivePartNames(ctx, hostName, tableState.DestinationTableName)
			if err != nil {
				return nil, xerrors.Errorf("verify old table: %w", err)
			}
			if partNamesAfterAttach != partNamesBeforeAttach {
				return nil, xerrors.New("old table parts changed during capture; discard assembly and retry snapshot")
			}
		}
		// on 'DisabledCleanup' check assembly_table, where already both: old rows from target_table & new rows
		if err := d.tableCommands.checkTableUUIDAndRowCounts(ctx, assemblyTable, expectedRowCountsByShard, false); err != nil {
			return nil, xerrors.Errorf("verify captured old table: %w", err)
		}
	}
	for _, currPart := range readyParts {
		if currPart.ShardCount() != len(d.tableCommands.tablesClient.NodesByShard()) {
			return nil, xerrors.New("part load attempt topology differs from snapshot topology")
		}
		partTableValue, _ := currPart.Shard(0)
		partTable := &partTableValue
		if partTable.ExpectedTableUUID == "" {
			return nil, xerrors.New("missing part table for snapshot load attempt")
		}
		partRowCountsByShard := make([]uint64, currPart.ShardCount())
		for shardIndex := 0; shardIndex < currPart.ShardCount(); shardIndex++ {
			shardPartTable, expectedPartRows := currPart.Shard(shardIndex)
			if shardPartTable.ExpectedTableUUID != partTable.ExpectedTableUUID {
				return nil, xerrors.New("replicated database part load attempt must use one shared staging table")
			}
			if ^uint64(0)-expectedRowCountsByShard[shardIndex] < expectedPartRows {
				return nil, xerrors.New("snapshot row count overflow")
			}
			partRowCountsByShard[shardIndex] = expectedPartRows
			expectedRowCountsByShard[shardIndex] += expectedPartRows
		}
		// second check of 'PartID' table
		if err := d.tableCommands.checkTableUUIDAndRowCounts(ctx, partTable, partRowCountsByShard, true); err != nil {
			return nil, xerrors.Errorf("verify shared staging table: %w", err)
		}
		if strings.HasPrefix(assemblyEngine, "VersionedCollapsingMergeTree(") {
			err = d.insertStagingRowsIntoAssemblies(ctx, assemblyTable, partTable.TableName)
		} else {
			err = d.attachSharedTablePartitions(ctx, assemblyTable, partTable.TableName)
		}
		if err != nil {
			return nil, xerrors.Errorf("import shared staging table: %w", err)
		}
	}
	// check assembly_table
	if err := d.tableCommands.checkTableUUIDAndRowCounts(ctx, assemblyTable, expectedRowCountsByShard, false); err != nil {
		return nil, xerrors.Errorf("verify shared assembly: %w", err)
	}
	candidateTable := ch_eo_common.NewTemporaryTable(tableState.TemporaryTablePrefix, assemblyTable.CommandHostName, ch_eo_common.RoleCandidate)
	*cleanupTables = append(*cleanupTables, candidateTable)
	candidateEngine := assemblyEngine
	if tableState.UseReplicatedTableEngine {
		candidateEngine = ch_eo_common.BuildReplicatedEngineWithMacros(assemblyEngine, d.transferID, candidateTable.ExpectedTableUUID)
	}
	candidateDDL, err := ch_eo_common.BuildCreateTableDDL(d.tableCommands.databaseName, candidateTable.TableName, candidateTable.ExpectedTableUUID, tableState.DDLTemplate, candidateEngine)
	if err != nil {
		return nil, xerrors.Errorf("build shared candidate DDL: %w", err)
	}
	if err := d.tableCommands.executeOwnedTableCommand(ctx, candidateTable, candidateDDL); err != nil {
		return nil, xerrors.Errorf("create shared candidate: %w", err)
	}
	candidateMergeHosts, err := d.stopPrivateTableMerges(ctx, candidateTable)
	if err != nil {
		return nil, xerrors.Errorf("stop shared candidate merges: %w", err)
	}
	if tableState.UseReplicatedTableEngine {
		if err := d.tableCommands.validateTableReplicaTopology(ctx, candidateTable.TableName); err != nil {
			return nil, xerrors.Errorf("check shared candidate replicas: %w", err)
		}
	}
	if err := d.attachSharedTablePartitions(ctx, candidateTable, assemblyTable.TableName); err != nil {
		return nil, xerrors.Errorf("import shared assembly: %w", err)
	}
	// check 'candidate_table'
	if err := d.tableCommands.checkTableUUIDAndRowCounts(ctx, candidateTable, expectedRowCountsByShard, tableState.UseReplicatedTableEngine); err != nil {
		return nil, xerrors.Errorf("verify shared candidate: %w", err)
	}
	if err := d.startPrivateTableMerges(ctx, candidateTable, candidateMergeHosts); err != nil {
		return nil, xerrors.Errorf("restart verified shared candidate merges: %w", err)
	}
	var candidates []ch_eo_common.NodeState
	for _, replica := range tableState.NodeStates {
		candidates = append(candidates, ch_eo_common.NewCandidate(replica.HostName, replica.ShardIndex, *candidateTable, expectedRowCountsByShard[replica.ShardIndex]))
	}
	return candidates, nil
}

func (d *replicatedOperations) insertStagingRowsIntoAssemblies(ctx context.Context, assemblyTable *ch_eo_common.OwnedTable, stagingTableName string) error {
	nodesByShard, err := d.SelectNodesByShard(ctx)
	if err != nil {
		return xerrors.Errorf("select assembly replicas: %w", err)
	}
	// INSERT is local to each non-replicated assembly. Keep commands sequential:
	// every copy shares the UUID used as the durable pending-command key.
	for _, shardNodes := range nodesByShard {
		for _, node := range shardNodes {
			if err := d.SyncDatabaseDDLOnNode(ctx, node.Host.Name); err != nil {
				return xerrors.Errorf("wait for assembly replica: %w", err)
			}
			localAssembly := *assemblyTable
			localAssembly.CommandHostName = node.Host.Name
			if err := d.tableCommands.insertAllRowsFromTable(ctx, &localAssembly, stagingTableName); err != nil {
				return xerrors.Errorf("sort staging rows on %s: %w", node.Host.Name, err)
			}
		}
	}
	return nil
}

func (d *replicatedOperations) attachSharedTablePartitions(ctx context.Context, destinationTable *ch_eo_common.OwnedTable, sourceTableName string) error {
	// Different shards may contain different partition IDs. An empty local
	// source partition contributes no rows; every existing ID must be imported.
	partitionIDsByShard := make([][]string, len(d.tableCommands.tablesClient.NodesByShard()))
	var partitionReads errgroup.Group
	partitionReads.SetLimit(32)
	for shardIndex, shardNodes := range d.tableCommands.tablesClient.NodesByShard() {
		partitionReads.Go(
			func() error {
				partitionIDs, err := d.tableCommands.tablesClient.PartitionIDs(ctx, shardNodes[0].Host.Name, sourceTableName)
				if err != nil {
					return xerrors.Errorf("list shared table partitions: %w", err)
				}
				partitionIDsByShard[shardIndex] = partitionIDs
				return nil
			},
		)
	}
	if err := partitionReads.Wait(); err != nil {
		return xerrors.Errorf("attachSharedTablePartitions: %w", err)
	}
	seenPartitionIDs := make(map[string]bool)
	for _, partitionIDs := range partitionIDsByShard {
		for _, partitionID := range partitionIDs {
			seenPartitionIDs[partitionID] = true
		}
	}
	partitionIDs := make([]string, 0, len(seenPartitionIDs))
	for partitionID := range seenPartitionIDs {
		partitionIDs = append(partitionIDs, partitionID)
	}
	sort.Strings(partitionIDs)
	for _, partitionID := range partitionIDs {
		attachPartitionSQL := fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION ID %s FROM %s", d.tableCommands.tablesClient.QualifiedTableName(destinationTable.TableName), ch_eo_common.QuoteStringLiteral(partitionID), d.tableCommands.tablesClient.QualifiedTableName(sourceTableName))
		if err := d.tableCommands.executeOwnedTableCommand(ctx, destinationTable, attachPartitionSQL); err != nil {
			return xerrors.Errorf("attach shared table partition: %w", err)
		}
	}
	return nil
}

// A Replicated database needs one EXCHANGE submission, not one per replica.
// A missing initiator is safe only when another host proves its candidate UUID.
func (d *replicatedOperations) Publish(ctx context.Context) error {
	tableState := d.tableCommands.table
	if err := d.WaitForDatabaseDDL(ctx); err != nil {
		return xerrors.Errorf("Publish: %w", err)
	}
	availableNodesByShard, err := d.tableCommands.tablesClient.SelectReplicas(ctx, true)
	if err != nil {
		return xerrors.Errorf("Publish: %w", err)
	}
	reachableHostNames := make(map[string]bool)
	publicationObserved := false
	expectedCandidateUUID := tableState.NodeStates[0].CandidateTable.ExpectedTableUUID
	for _, shardNodes := range availableNodesByShard {
		for _, node := range shardNodes {
			reachableHostNames[node.Host.Name] = true
			destinationTableUUID, err := d.tableCommands.tablesClient.TableUUID(ctx, node.Host.Name, tableState.DestinationTableName)
			if err != nil {
				return xerrors.Errorf("Publish: %w", err)
			}
			if destinationTableUUID == expectedCandidateUUID {
				publicationObserved = true
			}
		}
	}
	for _, nodeState := range tableState.NodeStates {
		if nodeState.PendingPublicationCommand == nil {
			continue
		}
		if reachableHostNames[nodeState.HostName] {
			if err := d.tableCommands.fenceCommand(ctx, nodeState.HostName, nodeState.PendingPublicationCommand); err != nil {
				return xerrors.Errorf("Publish: %w", err)
			}
		} else if !publicationObserved {
			return xerrors.New("publication initiator is unavailable and the EXCHANGE outcome is unresolved")
		}
		// The Command is submitted once. Observing its candidate UUID proves
		// it entered shared DDL; a disconnected response cannot submit it again.
		if err := d.tableCommands.recordCommand(ctx, nodeState.HostName, nil, nil); err != nil {
			return xerrors.Errorf("record publication resolution: %w", err)
		}
	}
	if !publicationObserved {
		expectedRowCountsByShard := make([]uint64, len(d.tableCommands.tablesClient.NodesByShard()))
		for _, nodeState := range tableState.NodeStates {
			expectedRowCountsByShard[nodeState.ShardIndex] = nodeState.ExpectedRowCount
		}
		candidateTable := tableState.NodeStates[0].CandidateTable
		if !tableState.PublicationDecided() {
			if err := d.tableCommands.checkTableUUIDAndRowCounts(ctx, &candidateTable, expectedRowCountsByShard, tableState.UseReplicatedTableEngine); err != nil {
				return xerrors.Errorf("Publish: %w", err)
			}
		}
		for nodeIndex, nodeState := range tableState.NodeStates {
			if !reachableHostNames[nodeState.HostName] {
				continue
			}
			if err := d.tableCommands.publishTableOnNode(ctx, tableState, nodeIndex); err != nil {
				return xerrors.Errorf("Publish: %w", err)
			}
			break
		}
	}
	// Refresh membership: a returned host must not be omitted from the final
	// barrier just because it was unavailable at the start of publication.
	if err := d.WaitForDatabaseDDL(ctx); err != nil {
		return xerrors.Errorf("Publish: %w", err)
	}
	if tableState.UseReplicatedTableEngine {
		if err := d.tableCommands.validateTableReplicaTopology(ctx, tableState.DestinationTableName); err != nil {
			return xerrors.Errorf("Publish: %w", err)
		}
	}
	availableNodesByShard, err = d.tableCommands.tablesClient.SelectReplicas(ctx, true)
	if err != nil {
		return xerrors.Errorf("Publish: %w", err)
	}
	for _, shardNodes := range availableNodesByShard {
		for _, node := range shardNodes {
			for _, nodeState := range tableState.NodeStates {
				if nodeState.HostName != node.Host.Name {
					continue
				}
				if tableState.UseReplicatedTableEngine {
					if err := d.tableCommands.executeCommandOverHTTP(ctx, node.Host.Name, "SYSTEM SYNC REPLICA "+d.tableCommands.tablesClient.QualifiedTableName(tableState.DestinationTableName), nil); err != nil {
						return xerrors.Errorf("Publish: %w", err)
					}
				}
				if tableState.PublicationDecided() {
					if err := d.tablesClient.CheckTableUUID(ctx, node.Host.Name, tableState.DestinationTableName, expectedCandidateUUID); err != nil {
						return xerrors.Errorf("verify published table identity: %w", err)
					}
				} else if err := d.tablesClient.CheckTableUUIDAndRowCount(ctx, node.Host.Name, tableState.DestinationTableName, expectedCandidateUUID, nodeState.ExpectedRowCount); err != nil {
					return xerrors.Errorf("Publish: %w", err)
				}
			}
		}
	}
	return nil
}

// A returned replica may win ATTACH before its ready table catches up. Discard
// an incomplete private assembly; never replay ATTACH into the same generation.
func (d *replicatedOperations) BuildCandidates(ctx context.Context, readyParts []ch_eo_common.ReadyPart) ([]ch_eo_common.NodeState, error) {
	tableState := d.tableCommands.table
	for buildAttemptIndex := 0; buildAttemptIndex < 3; buildAttemptIndex++ {
		var cleanupTables []*ch_eo_common.OwnedTable
		candidates, err := d.buildSharedCandidateTableOnce(ctx, tableState, readyParts, &cleanupTables)
		if err == nil {
			return candidates, nil
		}
		var rowCountMismatch *ch_eo_common.RowCountMismatch
		if !xerrors.As(err, &rowCountMismatch) || !rowCountMismatch.HasMissingRows() || len(d.tableCommands.pendingTableUUIDs()) != 0 || buildAttemptIndex == 2 {
			return nil, xerrors.Errorf("BuildCandidates: %w", err)
		}
		d.tableCommands.logger.Warn("Discarding incomplete snapshot assembly before rebuilding", log.Error(err))
		for _, temporaryTable := range cleanupTables {
			if err := d.tableCommands.dropOwnedTemporaryTable(ctx, temporaryTable); err != nil {
				return nil, xerrors.Errorf("BuildCandidates: %w", err)
			}
		}
	}
	return nil, xerrors.New("snapshot assembly attempts exhausted")
}

// Wait on every reachable database replica before interpreting UUIDs or retrying EXCHANGE.
// An expired HTTP session alone cannot fence the replicated DDL queue.
func (d *replicatedOperations) waitForDatabaseDDLOnce(ctx context.Context) error {
	ddlSyncs, ddlSyncCtx := errgroup.WithContext(ctx)
	// Waiting on replicas is independent. Bound concurrent requests instead
	// of multiplying their round-trip latency by the number of cluster nodes.
	ddlSyncs.SetLimit(8)
	availableNodesByShard, err := d.tableCommands.tablesClient.SelectReplicas(ctx, true)
	if err != nil {
		return xerrors.Errorf("waitForDatabaseDDLOnce: %w", err)
	}
	for _, shardNodes := range availableNodesByShard {
		for _, node := range shardNodes {
			hostName := node.Host.Name
			ddlSyncs.Go(
				func() error {
					if err := d.tableCommands.executeCommandOverHTTP(ddlSyncCtx, hostName, "SYSTEM SYNC DATABASE REPLICA "+ch_eo_common.QuoteIdentifier(d.tableCommands.databaseName), nil); err != nil {
						return xerrors.Errorf("wait for replicated database DDL on %s: %w", hostName, err)
					}
					return nil
				},
			)
		}
	}
	return ddlSyncs.Wait()
}

func (d *replicatedOperations) PartTableIdentityHostName(string) string {
	return d.tableCommands.tablesClient.NodesByShard()[0][0].Host.Name
}

func (d *replicatedOperations) newStagingEngineSQL() string {
	return ch_eo_common.BuildReplicatedEngineWithMacros("MergeTree()", d.transferID, uuid.NewString())
}

func (d *replicatedOperations) SelectNodesByShard(ctx context.Context) ([][]ch_eo_common.Node, error) {
	return d.tableCommands.tablesClient.SelectReplicas(ctx, true)
}

func (d *replicatedOperations) MissingNodeStates(tableState *ch_eo_common.SnapshotTable) []ch_eo_common.NodeState {
	var missingNodes []ch_eo_common.NodeState
	hostsWithState := make(map[string]bool)
	for _, nodeState := range tableState.NodeStates {
		hostsWithState[nodeState.HostName] = true
	}
	for shardIndex, shardNodes := range d.tableCommands.tablesClient.NodesByShard() {
		for _, node := range shardNodes {
			if !hostsWithState[node.Host.Name] {
				missingNodes = append(missingNodes, ch_eo_common.NewNodeState(node.Host.Name, shardIndex, tableState.NodeStates[0].PreviousTableUUID))
			}
		}
	}
	return missingNodes
}

func (d *replicatedOperations) resolveCommandHostName(ctx context.Context, _ string) (string, error) {
	availableNodesByShard, err := d.SelectNodesByShard(ctx)
	if err != nil {
		return "", err
	}
	return availableNodesByShard[0][0].Host.Name, nil
}

func (d *replicatedOperations) WaitForDatabaseDDL(ctx context.Context) error {
	return ch_eo_common.RetryReplicaBarrier(ctx, func() error { return d.waitForDatabaseDDLOnce(ctx) })
}

func (d *replicatedOperations) SyncDatabaseDDLOnNode(ctx context.Context, hostName string) error {
	return d.tableCommands.executeCommandOverHTTP(ctx, hostName, "SYSTEM SYNC DATABASE REPLICA "+ch_eo_common.QuoteIdentifier(d.tableCommands.databaseName), nil)
}

func (d *replicatedOperations) IsReplicaCountCompatibleWithTopology(reportedReplicaCount, configuredReplicaCount, _ int) bool {
	return reportedReplicaCount <= configuredReplicaCount
}

func (d *replicatedOperations) configureQueryParams(sqlStatement string, httpParams url.Values) {
	httpParams.Set("distributed_ddl_output_mode", "none")
	httpParams.Set("distributed_ddl_task_timeout", "0")
	if strings.HasPrefix(sqlStatement, "CREATE TABLE ") {
		httpParams.Set("database_replicated_allow_only_replicated_engine", "0")
		httpParams.Set("database_replicated_allow_explicit_uuid", "1")
		httpParams.Set("database_replicated_allow_replicated_engine_arguments", "1")
	}
	if strings.HasPrefix(sqlStatement, "ALTER TABLE ") {
		// ClickHouse 21.3 requires ALTER settings in HTTP parameters.
		httpParams.Set("replication_alter_partitions_sync", "1")
	}
}

func (d *replicatedOperations) EnsurePublicationBeforeCleanup(ctx context.Context) error {
	return d.Publish(ctx)
}

func (d *replicatedOperations) validateTopology(ctx context.Context, availableNodesByShard [][]ch_eo_common.Node) error {
	configuredHostCount := 0
	for _, shardNodes := range d.tableCommands.tablesClient.NodesByShard() {
		configuredHostCount += len(shardNodes)
	}
	expectedKeeperPath := ""
	seenReplicaPaths := make(map[string]bool)
	shardIndexByName := make(map[string]int)
	for shardIndex, shardNodes := range availableNodesByShard {
		expectedShardName := ""
		for _, node := range shardNodes {
			var keeperPath, replicaPath, shardName string
			var replicaCount uint64
			if err := d.tableCommands.tablesClient.ReadRow(ctx, node.Host.Name, "SELECT zookeeper_path, replica_path, shard_name, total_replicas FROM system.database_replicas WHERE database = ?", []any{d.tableCommands.databaseName}, &keeperPath, &replicaPath, &shardName, &replicaCount); err != nil {
				return xerrors.Errorf("read replicated database topology: %w", err)
			}
			if replicaCount != uint64(configuredHostCount) || (expectedKeeperPath != "" && expectedKeeperPath != keeperPath) || seenReplicaPaths[replicaPath] {
				return xerrors.New("snapshot must include every replica of one Replicated database exactly once")
			}
			if expectedShardName != "" && expectedShardName != shardName {
				return xerrors.New("ClickHouse shard grouping differs from Replicated database topology")
			}
			if previousShardIndex, found := shardIndexByName[shardName]; found && previousShardIndex != shardIndex {
				return xerrors.New("Replicated database shard appears in multiple destination shards")
			}
			expectedKeeperPath, expectedShardName = keeperPath, shardName
			seenReplicaPaths[replicaPath] = true
			shardIndexByName[shardName] = shardIndex
		}
	}
	return nil
}

func (d *replicatedOperations) ForTable(table *ch_eo_common.SnapshotTable, checkpoint CommandCheckpoint) DatabaseOperations {
	return newReplicated(d.tableCommands.logger, d.transferID, d.tableCommands.databaseName, d.cleanupPolicy, d.tableCommands.tablesClient, table, checkpoint)
}
