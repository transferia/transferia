package database_operations

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/errgroup"
)

type atomicOperations struct {
	transferID    string
	cleanupPolicy model.CleanupType
	// Atomic DDL is local. Skipped replicas may retain old data and temporary tables.
	requireAllReplicas bool
	*tableCommands
}

var _ DatabaseOperations = (*atomicOperations)(nil)

func newAtomic(logger log.Logger, transferID string, databaseName string, cleanupPolicy model.CleanupType, tablesClient *ch_eo_common.Client, table *ch_eo_common.SnapshotTable, checkpoint CommandCheckpoint) *atomicOperations {
	databaseOperations := &atomicOperations{
		transferID:         transferID,
		cleanupPolicy:      cleanupPolicy,
		requireAllReplicas: false,
		tableCommands:      nil,
	}
	databaseOperations.tableCommands = newTableCommands(logger, databaseName, tablesClient, table, databaseOperations, checkpoint)
	return databaseOperations
}

// BuildCandidates first combines rows in a local assembly table, then copies
// them into candidate tables with the destination engine on the required hosts.
// Variables named assembly refer to that intermediate table, not the candidates.
func (d *atomicOperations) BuildCandidates(ctx context.Context, readyParts []ch_eo_common.ReadyPart) ([]ch_eo_common.NodeState, error) {
	tableState := d.tableCommands.table
	_, assemblyEngine, _, err := ch_eo_common.ExtractTableDefinitionParts(tableState.DDLTemplate)
	if err != nil {
		return nil, xerrors.Errorf("BuildCandidates: %w", err)
	}
	assemblyEngine, err = ch_eo_common.MakeEngineNonReplicated(assemblyEngine)
	if err != nil {
		return nil, xerrors.Errorf("BuildCandidates: %w", err)
	}
	if err := d.WaitForPendingCommandsAndClearRecords(ctx); err != nil {
		return nil, xerrors.Errorf("fence previous shard build: %w", err)
	}

	availableHosts, err := d.availableHostNames(ctx)
	if err != nil {
		return nil, xerrors.Errorf("select candidate hosts: %w", err)
	}
	candidateResults := make([][]ch_eo_common.NodeState, len(d.tableCommands.tablesClient.NodesByShard()))
	var shardBuilds errgroup.Group
	shardBuilds.SetLimit(32)
	for shardIndex := range d.tableCommands.tablesClient.NodesByShard() {
		shardBuilds.Go(
			func() error {
				// Each shard builds private results; the owner serializes their durable registration.
				candidates, err := d.buildShardCandidates(ctx, tableState, readyParts, shardIndex, assemblyEngine, availableHosts)
				if err != nil {
					return xerrors.Errorf("build candidate shard %d: %w", shardIndex, err)
				}
				candidateResults[shardIndex] = candidates
				return nil
			},
		)
	}
	// A failed shard must not leave other workers running into publication or cleanup.
	if err := shardBuilds.Wait(); err != nil {
		return nil, xerrors.Errorf("build candidate shards: %w", err)
	}
	var candidates []ch_eo_common.NodeState
	for _, shardCandidates := range candidateResults {
		candidates = append(candidates, shardCandidates...)
	}
	return candidates, nil
}

func (d *atomicOperations) buildShardCandidatesOnce(ctx context.Context, tableState *ch_eo_common.SnapshotTable, readyParts []ch_eo_common.ReadyPart, shardIndex int, assemblyEngine string, availableHosts map[string]bool, cleanupTables *[]*ch_eo_common.OwnedTable, candidates *[]ch_eo_common.NodeState) error {
	candidateGenerationID := uuid.NewString()
	shardNodes := d.tableCommands.tablesClient.NodesByShard()[shardIndex]
	assemblyHostName := shardNodes[0].Host.Name
	assemblyTable := ch_eo_common.NewTemporaryTable(tableState.TemporaryTablePrefix, assemblyHostName, ch_eo_common.RoleAssembly)
	*cleanupTables = append(*cleanupTables, assemblyTable)
	assemblyDDL, err := ch_eo_common.BuildCreateTableDDL(d.tableCommands.databaseName, assemblyTable.TableName, assemblyTable.ExpectedTableUUID, tableState.DDLTemplate, assemblyEngine)
	if err != nil {
		return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
	}
	if err := d.tableCommands.executeOwnedTableCommand(ctx, assemblyTable, assemblyDDL); err != nil {
		return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
	}
	if err := d.tableCommands.stopPrivateTableMerges(ctx, assemblyTable); err != nil {
		return xerrors.Errorf("stop assembly merges: %w", err)
	}
	var expectedShardRowCount uint64
	if d.cleanupPolicy == model.DisabledCleanup {
		// on 'DisabledCleanup' we:
		//   - create assembly_table
		//   - ATTACH old partitions FROM target_table into assembly_table
		//   - ATTACH snapshot part table partitions into assembly_table
		//   - copy assembled data into candidate_table
		//   - publish candidate_table as target_table (EXCHANGE or RENAME)
		// ---
		// assembly_table - local MergeTree-family table without replication in the Atomic database
		// candidate_table - with target engine
		destinationTableUUID, err := d.tableCommands.tablesClient.TableUUID(ctx, assemblyHostName, tableState.DestinationTableName)
		if err != nil {
			return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
		}
		if destinationTableUUID != "" {
			activePartNamesBeforeAttach, err := d.tableCommands.tablesClient.JoinedActivePartNames(ctx, assemblyHostName, tableState.DestinationTableName)
			if err != nil {
				return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
			}
			expectedShardRowCount, err = d.tableCommands.tablesClient.CountPhysicalRows(ctx, assemblyHostName, tableState.DestinationTableName)
			if err != nil {
				return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
			}
			if err := d.tableCommands.attachAllPartitionsFromTable(ctx, assemblyTable, tableState.DestinationTableName); err != nil {
				return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
			}
			activePartNamesAfterAttach, err := d.tableCommands.tablesClient.JoinedActivePartNames(ctx, assemblyHostName, tableState.DestinationTableName)
			if err != nil {
				return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
			}
			if activePartNamesBeforeAttach != activePartNamesAfterAttach {
				return xerrors.New("old table parts changed during capture; discard assembly and retry snapshot")
			}
			// on 'cleanupPolicy == model.DisabledCleanup' -- check assembly_table (old rows from target + new rows)
			if err := d.tableCommands.tablesClient.CheckTableUUIDAndRowCount(ctx, assemblyHostName, assemblyTable.TableName, assemblyTable.ExpectedTableUUID, expectedShardRowCount); err != nil {
				return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
			}
		}
	}
	for _, currPart := range readyParts {
		if currPart.ShardCount() != len(d.tableCommands.tablesClient.NodesByShard()) {
			return xerrors.New("part load attempt topology differs from snapshot topology")
		}
		partTableValue, expectedPartRows := currPart.Shard(shardIndex)
		partTable := &partTableValue
		if partTable.ExpectedTableUUID == "" {
			return xerrors.New("missing part table for snapshot load attempt")
		}
		if partTable.CommandHostName != assemblyHostName {
			return xerrors.New("part load attempt belongs to a different ClickHouse host")
		}
		// check one more time that 'partID' still same UUID & same amount of rows
		if err := d.tableCommands.tablesClient.CheckTableUUIDAndRowCount(ctx, assemblyHostName, partTable.TableName, partTable.ExpectedTableUUID, expectedPartRows); err != nil {
			return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
		}
		if strings.HasPrefix(assemblyEngine, "VersionedCollapsingMergeTree(") {
			err = d.tableCommands.insertAllRowsFromTable(ctx, assemblyTable, partTable.TableName)
		} else {
			err = d.tableCommands.attachAllPartitionsFromTable(ctx, assemblyTable, partTable.TableName)
		}
		if err != nil {
			return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
		}
		if ^uint64(0)-expectedShardRowCount < expectedPartRows {
			return xerrors.New("snapshot row count overflow")
		}
		expectedShardRowCount += expectedPartRows
	}
	// check fully-built assembly_table of current shard
	if err := d.tableCommands.tablesClient.CheckTableUUIDAndRowCount(ctx, assemblyHostName, assemblyTable.TableName, assemblyTable.ExpectedTableUUID, expectedShardRowCount); err != nil {
		return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
	}
	var candidateOnAssemblyHost *ch_eo_common.OwnedTable
	for _, nodeState := range tableState.NodeStates {
		if nodeState.ShardIndex != shardIndex || !availableHosts[nodeState.HostName] {
			continue
		}
		candidateTable := ch_eo_common.NewTemporaryTable(tableState.TemporaryTablePrefix, nodeState.HostName, ch_eo_common.RoleCandidate)
		*cleanupTables = append(*cleanupTables, candidateTable)
		candidateEngine := assemblyEngine
		if tableState.UseReplicatedTableEngine {
			// Keep the generation in the table node itself: DROP removes that
			// node, but would leave empty per-generation parent directories.
			candidateKeeperPath := fmt.Sprintf("/clickhouse/tables/__dt_snapshot/%s_%s_%d", d.transferID, candidateGenerationID, shardIndex)
			candidateEngine = ch_eo_common.BuildReplicatedEngineSQL(assemblyEngine, candidateKeeperPath, nodeState.HostName)
		}
		candidateDDL, err := ch_eo_common.BuildCreateTableDDL(d.tableCommands.databaseName, candidateTable.TableName, candidateTable.ExpectedTableUUID, tableState.DDLTemplate, candidateEngine)
		if err != nil {
			return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
		}
		existingCandidateUUID, err := d.tableCommands.tablesClient.TableUUID(ctx, nodeState.HostName, candidateTable.TableName)
		if err != nil {
			if d.canSkipHost(ctx, nodeState.HostName, err) {
				continue
			}
			return xerrors.Errorf("read candidate before creation: %w", err)
		}
		if existingCandidateUUID == "" {
			if err := d.tableCommands.executeOwnedTableCommand(ctx, candidateTable, candidateDDL); err != nil {
				if !d.tableCommands.checkpointFailed() && d.canSkipHost(ctx, nodeState.HostName, err) {
					continue
				}
				return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
			}
		} else {
			return xerrors.New("candidate table already exists with an unexpected UUID")
		}
		if err := d.tableCommands.stopPrivateTableMerges(ctx, candidateTable); err != nil {
			if !d.tableCommands.checkpointFailed() && d.canSkipHost(ctx, nodeState.HostName, err) {
				continue
			}
			return xerrors.Errorf("stop candidate merges: %w", err)
		}
		*candidates = append(*candidates, ch_eo_common.NewCandidate(nodeState.HostName, shardIndex, *candidateTable, expectedShardRowCount))
		if nodeState.HostName == assemblyHostName {
			candidateOnAssemblyHost = candidateTable
		}
	}
	if candidateOnAssemblyHost == nil {
		return xerrors.New("candidate table on assembly host is absent")
	}
	if tableState.UseReplicatedTableEngine {
		partitionIDs, err := d.tableCommands.tablesClient.PartitionIDs(ctx, assemblyHostName, assemblyTable.TableName)
		if err != nil {
			return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
		}
		for _, partitionID := range partitionIDs {
			replacePartitionSQL := fmt.Sprintf("ALTER TABLE %s REPLACE PARTITION ID %s FROM %s", d.tableCommands.tablesClient.QualifiedTableName(candidateOnAssemblyHost.TableName), ch_eo_common.QuoteStringLiteral(partitionID), d.tableCommands.tablesClient.QualifiedTableName(assemblyTable.TableName))
			if err := d.tableCommands.executeOwnedTableCommand(ctx, candidateOnAssemblyHost, replacePartitionSQL); err != nil {
				return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
			}
		}
	} else if err := d.tableCommands.attachAllPartitionsFromTable(ctx, candidateOnAssemblyHost, assemblyTable.TableName); err != nil {
		return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
	}
	verifiedCandidates := make([]ch_eo_common.NodeState, 0, len(*candidates))
	for _, nodeState := range *candidates {
		if tableState.UseReplicatedTableEngine {
			if err := d.tableCommands.executeCommandOverHTTP(ctx, nodeState.HostName, "SYSTEM SYNC REPLICA "+d.tableCommands.tablesClient.QualifiedTableName(nodeState.CandidateTable.TableName), nil); err != nil {
				if d.canSkipHost(ctx, nodeState.HostName, err) {
					continue
				}
				return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
			}
		}
		// check candidate_table on everry node of current shard
		if err := d.tableCommands.tablesClient.CheckTableUUIDAndRowCount(ctx, nodeState.HostName, nodeState.CandidateTable.TableName, nodeState.CandidateTable.ExpectedTableUUID, expectedShardRowCount); err != nil {
			if d.canSkipHost(ctx, nodeState.HostName, err) {
				continue
			}
			return xerrors.Errorf("buildShardCandidatesOnce: %w", err)
		}
		verifiedCandidates = append(verifiedCandidates, nodeState)
	}
	if len(verifiedCandidates) == 0 {
		return xerrors.Errorf("no verified candidate on shard %d", shardIndex)
	}
	var releasedCandidates []ch_eo_common.NodeState
	for _, nodeState := range verifiedCandidates {
		candidate := nodeState.CandidateTable
		if err := d.tableCommands.startPrivateTableMerges(ctx, &candidate); err != nil {
			if !d.tableCommands.checkpointFailed() && d.canSkipHost(ctx, nodeState.HostName, err) {
				continue
			}
			return xerrors.Errorf("restart verified candidate merges: %w", err)
		}
		releasedCandidates = append(releasedCandidates, nodeState)
	}
	if len(releasedCandidates) == 0 {
		return xerrors.Errorf("no candidate with resumed merges on shard %d", shardIndex)
	}
	*candidates = releasedCandidates
	return nil
}

func (d *atomicOperations) buildShardCandidates(ctx context.Context, tableState *ch_eo_common.SnapshotTable, readyParts []ch_eo_common.ReadyPart, shardIndex int, assemblyEngine string, availableHosts map[string]bool) ([]ch_eo_common.NodeState, error) {
	for buildAttemptNumber := 1; buildAttemptNumber <= 3; buildAttemptNumber++ {
		var cleanupTables []*ch_eo_common.OwnedTable
		var candidates []ch_eo_common.NodeState
		err := d.buildShardCandidatesOnce(ctx, tableState, readyParts, shardIndex, assemblyEngine, availableHosts, &cleanupTables, &candidates)
		if err == nil {
			return candidates, nil
		}
		stateWritesBlocked := d.tableCommands.checkpointFailed()
		if stateWritesBlocked || ctx.Err() != nil || !ch_eo_common.IsRetryableReadError(err) || buildAttemptNumber == 3 {
			return nil, xerrors.Errorf("buildShardCandidates: %w", err)
		}
		// Never replay an ambiguous ATTACH into the same generation. Fencing and
		// owned-UUID deletion must succeed before any replacement is built.
		for _, temporaryTable := range cleanupTables {
			if cleanupErr := d.tableCommands.dropOwnedTemporaryTable(ctx, temporaryTable); cleanupErr != nil {
				return nil, xerrors.Errorf("discard failed shard build (%v): %w", err, cleanupErr)
			}
		}
		d.tableCommands.logger.Warn("Retrying ClickHouse snapshot shard with fresh tables", log.Int("shard", shardIndex), log.Int("Attempt", buildAttemptNumber), log.Error(err))
		if err := ch_eo_common.WaitOrCancel(ctx, time.Second); err != nil {
			return nil, err
		}
	}
	return nil, xerrors.New("snapshot shard build attempts exhausted")
}

func (d *atomicOperations) PartTableIdentityHostName(hostName string) string {
	return hostName
}

func (d *atomicOperations) newStagingEngineSQL() string {
	return "MergeTree()"
}

func (d *atomicOperations) SelectNodesByShard(ctx context.Context) ([][]ch_eo_common.Node, error) {
	return d.tableCommands.tablesClient.SelectReplicas(ctx, !d.requireAllReplicas)
}

func (d *atomicOperations) MissingNodeStates(*ch_eo_common.SnapshotTable) []ch_eo_common.NodeState {
	return nil
}

func (d *atomicOperations) resolveCommandHostName(_ context.Context, hostName string) (string, error) {
	return hostName, nil
}

func (d *atomicOperations) WaitForDatabaseDDL(context.Context) error {
	return nil
}

func (d *atomicOperations) SyncDatabaseDDLOnNode(context.Context, string) error {
	return nil
}

func (d *atomicOperations) IsReplicaCountCompatibleWithTopology(reportedReplicaCount, configuredReplicaCount, availableReplicaCount int) bool {
	if !d.requireAllReplicas {
		return reportedReplicaCount >= availableReplicaCount && reportedReplicaCount <= configuredReplicaCount
	}
	return reportedReplicaCount <= configuredReplicaCount && reportedReplicaCount == availableReplicaCount
}

func (d *atomicOperations) configureQueryParams(sqlStatement string, httpParams url.Values) {
	if strings.HasPrefix(sqlStatement, "ALTER TABLE ") {
		// ClickHouse 21.3 requires ALTER settings in HTTP parameters.
		syncReplicas := "2"
		if !d.requireAllReplicas {
			syncReplicas = "1"
		}
		httpParams.Set("replication_alter_partitions_sync", syncReplicas)
	}
}

// Publish switches every available verified candidate. An unavailable Atomic
// replica does not catch up its table name automatically when it returns.
func (d *atomicOperations) Publish(ctx context.Context) error {
	availableHosts, err := d.availableHostNames(ctx)
	if err != nil {
		return xerrors.Errorf("select publication hosts: %w", err)
	}
	publishedNodes := make([]bool, len(d.table.NodeStates))
	var publications errgroup.Group
	publications.SetLimit(32)
	for nodeIndex, nodeState := range d.table.NodeStates {
		if !availableHosts[nodeState.HostName] || (!d.requireAllReplicas && nodeState.CandidateTable.ExpectedTableUUID == "") {
			continue
		}
		publications.Go(func() error {
			if err := d.publishTableOnNode(ctx, d.table, nodeIndex); err != nil {
				if !d.checkpointFailed() && d.canSkipHost(ctx, nodeState.HostName, err) {
					return nil
				}
				return xerrors.Errorf("publish Atomic table: %w", err)
			}
			if !d.requireAllReplicas && !d.table.PublicationDecided() {
				if err := d.tablesClient.CheckTableUUIDAndRowCount(ctx, nodeState.HostName, d.table.DestinationTableName, nodeState.CandidateTable.ExpectedTableUUID, nodeState.ExpectedRowCount); err != nil {
					if d.canSkipHost(ctx, nodeState.HostName, err) {
						return nil
					}
					return xerrors.Errorf("verify published Atomic table: %w", err)
				}
			}
			publishedNodes[nodeIndex] = true
			return nil
		})
	}
	// Do not start cleanup while any publication command is still running.
	if err := publications.Wait(); err != nil {
		return xerrors.Errorf("publish Atomic replicas: %w", err)
	}
	publishedByShard := make([]bool, len(d.tablesClient.NodesByShard()))
	for nodeIndex, nodeState := range d.table.NodeStates {
		if publishedNodes[nodeIndex] {
			publishedByShard[nodeState.ShardIndex] = true
		}
	}
	for shardIndex, published := range publishedByShard {
		if !published {
			return xerrors.Errorf("no published snapshot replica on shard %d", shardIndex)
		}
	}
	return nil
}

func (d *atomicOperations) EnsurePublicationBeforeCleanup(ctx context.Context) error {
	if d.requireAllReplicas {
		return nil
	}
	return d.Publish(ctx)
}

func (d *atomicOperations) validateTopology(availableNodesByShard [][]ch_eo_common.Node) error {
	for shardIndex, shardNodes := range availableNodesByShard {
		if len(shardNodes) == 0 {
			return xerrors.Errorf("Atomic snapshot has no available replica on shard %d", shardIndex)
		}
		if d.requireAllReplicas && len(shardNodes) != len(d.tableCommands.tablesClient.NodesByShard()[shardIndex]) {
			return xerrors.New("Atomic snapshot requires every configured replica")
		}
	}
	return nil
}

func (d *atomicOperations) ForTable(table *ch_eo_common.SnapshotTable, checkpoint CommandCheckpoint) DatabaseOperations {
	operations := newAtomic(d.tableCommands.logger, d.transferID, d.tableCommands.databaseName, d.cleanupPolicy, d.tableCommands.tablesClient, table, checkpoint)
	operations.requireAllReplicas = d.requireAllReplicas
	return operations
}

func (d *atomicOperations) availableHostNames(ctx context.Context) (map[string]bool, error) {
	nodesByShard, err := d.SelectNodesByShard(ctx)
	if err != nil {
		return nil, xerrors.Errorf("select available Atomic replicas: %w", err)
	}
	hostNames := make(map[string]bool)
	for _, shardNodes := range nodesByShard {
		for _, node := range shardNodes {
			hostNames[node.Host.Name] = true
		}
	}
	return hostNames, nil
}

// Only a confirmed unavailable host may be skipped. SQL errors, incorrect
// UUIDs/counts, canceled operations and failed checkpoints remain failures.
func (d *atomicOperations) canSkipHost(ctx context.Context, hostName string, commandErr error) bool {
	if d.requireAllReplicas || ctx.Err() != nil || !ch_eo_common.IsRetryableReadError(commandErr) {
		return false
	}
	hostNames, err := d.availableHostNames(ctx)
	if err != nil || hostNames[hostName] {
		return false
	}
	d.logger.Warn("Skipping unavailable Atomic replica; old data and temporary tables may remain", log.String("host", hostName), log.Error(commandErr))
	return true
}

// Cleanup deliberately leaves resources and pending commands on unavailable
// Atomic replicas. Their names are not replicated, so no later catch-up is promised.
func (d *atomicOperations) availableTableCommands(ctx context.Context) (*tableCommands, error) {
	if d.requireAllReplicas {
		return d.tableCommands, nil
	}
	hostNames, err := d.availableHostNames(ctx)
	if err != nil {
		return nil, xerrors.Errorf("select Atomic cleanup hosts: %w", err)
	}
	table := *d.table
	table.NodeStates = make([]ch_eo_common.NodeState, len(d.table.NodeStates))
	availableNodeCount := 0
	for _, node := range d.table.NodeStates {
		if hostNames[node.HostName] {
			table.NodeStates[availableNodeCount] = node
			availableNodeCount++
		}
	}
	table.NodeStates = table.NodeStates[:availableNodeCount]
	table.PendingTableCommands = make(map[string]*ch_eo_common.OwnedTable)
	for tableUUID, resource := range d.table.PendingTableCommands {
		if hostNames[resource.CommandHostName] {
			table.PendingTableCommands[tableUUID] = resource
		}
	}
	return newTableCommands(d.logger, d.databaseName, d.tablesClient, &table, d, d.checkpoint), nil
}

func (d *atomicOperations) WaitForPendingCommandsAndClearRecords(ctx context.Context) error {
	commands, err := d.availableTableCommands(ctx)
	if err != nil {
		return xerrors.Errorf("select pending Atomic commands: %w", err)
	}
	for _, tableUUID := range commands.pendingTableUUIDs() {
		resource := commands.pendingTable(tableUUID)
		if err := commands.fencePendingTableCommand(ctx, tableUUID); err != nil {
			if !commands.checkpointFailed() && d.canSkipHost(ctx, resource.CommandHostName, err) {
				continue
			}
			return xerrors.Errorf("wait for pending Atomic command: %w", err)
		}
	}
	return nil
}

func (d *atomicOperations) DropPreviousTables(ctx context.Context) error {
	commands, err := d.availableTableCommands(ctx)
	if err != nil {
		return xerrors.Errorf("select previous Atomic tables: %w", err)
	}
	var drops errgroup.Group
	drops.SetLimit(32)
	for _, node := range commands.table.NodeStates {
		if node.CandidateTable.ExpectedTableUUID == "" || node.PreviousTableUUID == "" {
			continue
		}
		drops.Go(func() error {
			previous := node.CandidateTable
			previous.ExpectedTableUUID = node.PreviousTableUUID
			if err := commands.dropOwnedTemporaryTable(ctx, &previous); err != nil {
				if !commands.checkpointFailed() && d.canSkipHost(ctx, node.HostName, err) {
					return nil
				}
				return xerrors.Errorf("drop previous Atomic table: %w", err)
			}
			return nil
		})
	}
	if err := drops.Wait(); err != nil {
		return xerrors.Errorf("drop previous Atomic tables: %w", err)
	}
	return nil
}

func (d *atomicOperations) PreparePreviousPublicationForCleanup(ctx context.Context) error {
	commands, err := d.availableTableCommands(ctx)
	if err != nil {
		return xerrors.Errorf("select previous Atomic publication: %w", err)
	}
	return commands.PreparePreviousPublicationForCleanup(ctx)
}

func (d *atomicOperations) DropTemporaryTables(ctx context.Context) error {
	// A replica can disappear between discovery and DROP. A new discovery
	// skips it; SQL/ownership/checkpoint errors must never take this path.
	attemptLimit := 1
	for _, shardNodes := range d.tablesClient.NodesByShard() {
		attemptLimit += len(shardNodes)
	}
	for attemptNumber := 1; attemptNumber <= attemptLimit; attemptNumber++ {
		commands, err := d.availableTableCommands(ctx)
		if err != nil {
			return xerrors.Errorf("select temporary Atomic tables: %w", err)
		}
		if commands.table.TemporaryTablePrefix == "" {
			return xerrors.New("snapshot temporary table prefix is missing")
		}
		nodesByShard, err := d.SelectNodesByShard(ctx)
		if err != nil {
			return xerrors.Errorf("discover Atomic cleanup replicas: %w", err)
		}
		var drops errgroup.Group
		drops.SetLimit(32)
		for _, shardNodes := range nodesByShard {
			for _, node := range shardNodes {
				drops.Go(func() error { return commands.dropTemporaryTablesOnNode(ctx, d.table, node) })
			}
		}
		err = drops.Wait()
		if err == nil {
			return nil
		}
		if d.requireAllReplicas || commands.checkpointFailed() || ctx.Err() != nil || !ch_eo_common.IsRetryableReadError(err) || attemptNumber == attemptLimit {
			return xerrors.Errorf("drop temporary Atomic tables: %w", err)
		}
	}
	return xerrors.New("Atomic temporary table cleanup attempts exhausted")
}
