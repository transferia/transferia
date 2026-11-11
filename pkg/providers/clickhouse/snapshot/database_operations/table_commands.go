package database_operations

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/transferia/transferia/library/go/core/xerrors"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/errgroup"
)

// tableCommands coordinates durable commands and database-specific barriers.
// The lower tables layer never calls a database strategy.
type tableCommands struct {
	logger             log.Logger
	databaseName       string
	tablesClient       *ch_eo_common.Client
	table              *ch_eo_common.SnapshotTable
	databaseOperations DatabaseOperations
	checkpointMutex    sync.Mutex
	checkpoint         CommandCheckpoint
	checkpointError    error
}

func newTableCommands(logger log.Logger, databaseName string, tablesClient *ch_eo_common.Client, table *ch_eo_common.SnapshotTable, databaseOperations DatabaseOperations, checkpoint CommandCheckpoint) *tableCommands {
	return &tableCommands{logger: logger, databaseName: databaseName, tablesClient: tablesClient, table: table, databaseOperations: databaseOperations, checkpointMutex: sync.Mutex{}, checkpoint: checkpoint, checkpointError: nil}
}

const (
	unknownSettingCode = 115
	sessionLockedCode  = 373
)

func (s *tableCommands) executeCommandOverHTTP(ctx context.Context, hostName string, sqlStatement string, httpParams url.Values) error {
	if httpParams == nil {
		httpParams = make(url.Values)
	}
	s.databaseOperations.configureQueryParams(sqlStatement, httpParams)
	err := s.tablesClient.ExecuteCommandOverHTTP(ctx, hostName, sqlStatement, httpParams)
	var serverError *clickhouse_go.Exception
	if xerrors.As(err, &serverError) && serverError.Code == unknownSettingCode && strings.Contains(serverError.Message, "max_table_size_to_drop") && httpParams.Has("max_table_size_to_drop") {
		// On 21.3 this is a server setting, not a query setting. The rejected
		// request has not executed DROP. Retry without the unsupported override;
		// the server's size limit still applies and its errors must be returned.
		httpParams.Del("max_table_size_to_drop")
		err = s.tablesClient.ExecuteCommandOverHTTP(ctx, hostName, sqlStatement, httpParams)
	}
	return err
}

func (s *tableCommands) fenceCommand(ctx context.Context, hostName string, command *ch_eo_common.Command) error {
	if err := s.tablesClient.FenceSession(ctx, hostName, command.SessionID); err != nil {
		return xerrors.Errorf("fenceCommand: %w", err)
	}
	if !ch_eo_common.CommandRequiresDatabaseDDLFence(command.SQL) {
		return nil
	}
	// The initiating query may have queued DDL which outlives its HTTP session.
	return s.databaseOperations.WaitForDatabaseDDL(ctx)
}

func (s *tableCommands) executeAndFenceCommand(ctx context.Context, hostName string, command *ch_eo_common.Command) error {
	httpParams := ch_eo_common.ExistingSessionParams(command.SessionID)
	httpParams.Set("query_id", command.SessionID)
	if strings.HasPrefix(command.SQL, "DROP TABLE ") {
		// The caller verified the expected UUID before preparing this deletion.
		httpParams.Set("max_table_size_to_drop", "0")
	}
	progressCtx, stopProgress := context.WithCancel(ctx)
	defer stopProgress()

	requestCtx, cancelHTTPWait := context.WithCancel(ctx)
	defer cancelHTTPWait()

	// Monitors the SQL command while the caller waits for the HTTP response.
	// Logs query and replica-fetch progress every 10 seconds.
	// Cancels the HTTP wait if no active query is observed for 3 minutes.
	// Does not confirm success; the result is checked separately.
	go s.tablesClient.MonitorCommandProgress(progressCtx, hostName, command, cancelHTTPWait)

	err := s.executeCommandOverHTTP(requestCtx, hostName, command.SQL, httpParams)

	if err != nil {
		s.logger.Warn("ClickHouse snapshot command requires reconciliation", log.String("host", hostName), log.Error(err))
	}
	if fenceErr := s.fenceCommand(ctx, hostName, command); fenceErr != nil {
		return xerrors.Errorf("fence command on %s: %w", hostName, fenceErr)
	}
	if err != nil {
		return xerrors.Errorf("executeAndFenceCommand: %w", err)
	}
	return nil
}

// executeOwnedTableCommand is used on private staging resources only. An ambiguous assembly is
// discarded wholesale; ATTACH/INSERT is never replayed into that assembly.

func (s *tableCommands) executeOwnedTableCommand(ctx context.Context, ownedTable *ch_eo_common.OwnedTable, sqlStatement string) error {
	tableCommand, err := s.tablesClient.PrepareSessionCommand(ctx, ownedTable.CommandHostName, sqlStatement)
	if err != nil {
		return xerrors.Errorf("executeOwnedTableCommand: %w", err)
	}
	tableCommand.ReplicaFetchTableName = ownedTable.TableName
	if err := s.recordCommand(ctx, ownedTable.CommandHostName, ownedTable, tableCommand); err != nil {
		return xerrors.Errorf("executeOwnedTableCommand: %w", err)
	}
	if err := s.executeAndFenceCommand(ctx, ownedTable.CommandHostName, tableCommand); err != nil {
		return xerrors.Errorf("executeOwnedTableCommand: %w", err)
	}
	return s.recordCommand(ctx, ownedTable.CommandHostName, ownedTable, nil)
}

// Secondary workers share a per-table session on each host. A locked session
// rejected the query before execution; other errors must fail the whole attempt.
// Only main creates these sessions. Cleanup closes them before discovering data
// tables, which also rejects a delayed CREATE arriving after the discovery scan.

func (s *tableCommands) executeStagingCommand(ctx context.Context, hostName string, statement string) error {
	tableState := s.table
	if tableState == nil {
		return xerrors.New("staging DDL requires initialized table state")
	}
	sessionID := tableState.StagingDDLSessionIDs[hostName]
	if sessionID == "" {
		return xerrors.New("staging DDL session is missing")
	}
	httpParams := ch_eo_common.ExistingSessionParams(sessionID)
	httpParams.Set("session_timeout", "60")
	httpParams.Set("query_id", uuid.NewString())
	for {
		err := s.executeCommandOverHTTP(ctx, hostName, statement, httpParams)
		var serverError *clickhouse_go.Exception
		if err == nil {
			return s.databaseOperations.WaitForDatabaseDDL(ctx)
		}
		if !xerrors.As(err, &serverError) || serverError.Code != sessionLockedCode {
			return xerrors.Errorf("executeStagingCommand: %w", err)
		}
		if err := ch_eo_common.WaitOrCancel(ctx, 100*time.Millisecond); err != nil {
			return xerrors.Errorf("executeStagingCommand: %w", err)
		}
	}
}

func (s *tableCommands) fenceStagingCommands(ctx context.Context, tableState *ch_eo_common.SnapshotTable) error {
	if tableState.StagingDDLSessionIDs == nil {
		if err := s.databaseOperations.WaitForDatabaseDDL(ctx); err != nil {
			return xerrors.Errorf("fenceStagingCommands: %w", err)
		}
		return nil
	}
	for hostName, sessionID := range tableState.StagingDDLSessionIDs {
		if err := s.tablesClient.FenceSession(ctx, hostName, sessionID); err != nil {
			return xerrors.Errorf("fenceStagingCommands: %w", err)
		}
	}
	if err := s.databaseOperations.WaitForDatabaseDDL(ctx); err != nil {
		return xerrors.Errorf("fenceStagingCommands: %w", err)
	}
	return nil
}

func (s *tableCommands) attachAllPartitionsFromTable(ctx context.Context, destinationTable *ch_eo_common.OwnedTable, sourceTableName string) error {
	partitionIDs, err := s.tablesClient.PartitionIDs(ctx, destinationTable.CommandHostName, sourceTableName)
	if err != nil {
		return xerrors.Errorf("attachAllPartitionsFromTable: %w", err)
	}
	for _, partitionID := range partitionIDs {
		attachPartitionSQL := fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION ID %s FROM %s", s.tablesClient.QualifiedTableName(destinationTable.TableName), ch_eo_common.QuoteStringLiteral(partitionID), s.tablesClient.QualifiedTableName(sourceTableName))
		if err := s.executeOwnedTableCommand(ctx, destinationTable, attachPartitionSQL); err != nil {
			return xerrors.Errorf("attachAllPartitionsFromTable: %w", err)
		}
	}
	return nil
}

// VersionedCollapsing appends its version column to the effective sorting key.
// INSERT sorts plain MergeTree staging rows by that key; ATTACH keeps their old order.
// Preserve physical rows during that sort: STOP MERGES does not disable in-block collapsing.
func (s *tableCommands) insertAllRowsFromTable(ctx context.Context, destinationTable *ch_eo_common.OwnedTable, sourceTableName string) error {
	insertSQL := "INSERT INTO " + s.tablesClient.QualifiedTableName(destinationTable.TableName) + " SELECT * FROM " + s.tablesClient.QualifiedTableName(sourceTableName) + " SETTINGS insert_deduplicate=0, optimize_on_insert=0, max_partitions_per_insert_block=0"
	if err := s.executeOwnedTableCommand(ctx, destinationTable, insertSQL); err != nil {
		return xerrors.Errorf("insertAllRowsFromTable: %w", err)
	}
	return nil
}

// publishTableOnNode - makes 'EXCHANGE TABLES' on ch-node
// Atomic databases require a separate EXCHANGE/RENAME on each host.
// A Replicated database broadcasts the first command to every host. After the
// DDL queue is drained, subsequent hosts observe the candidate UUID and only
// record completion; repeating EXCHANGE would restore the old table.
func (s *tableCommands) publishTableOnNode(ctx context.Context, tableState *ch_eo_common.SnapshotTable, nodeIndex int) error {
	for {
		nodeState := tableState.NodeStates[nodeIndex]
		if nodeState.PendingPublicationCommand != nil {
			if err := s.fenceCommand(ctx, nodeState.HostName, nodeState.PendingPublicationCommand); err != nil {
				return xerrors.Errorf("publishTableOnNode: %w", err)
			}
		}
		actualDestinationUUID, err := s.tablesClient.TableUUID(ctx, nodeState.HostName, tableState.DestinationTableName)
		if err != nil {
			return xerrors.Errorf("publishTableOnNode: %w", err)
		}
		if actualDestinationUUID == nodeState.CandidateTable.ExpectedTableUUID {
			if err := s.recordCommand(ctx, nodeState.HostName, nil, nil); err != nil {
				return xerrors.Errorf("record published UUID: %w", err)
			}
			return nil
		}
		if actualDestinationUUID != nodeState.PreviousTableUUID {
			return xerrors.Errorf("unexpected live UUID on %s/%s: %s", nodeState.HostName, tableState.DestinationTableName, actualDestinationUUID)
		}
		actualCandidateUUID, err := s.tablesClient.TableUUID(ctx, nodeState.HostName, nodeState.CandidateTable.TableName)
		if err != nil {
			return xerrors.Errorf("publishTableOnNode: %w", err)
		}
		if actualCandidateUUID != nodeState.CandidateTable.ExpectedTableUUID {
			return xerrors.Errorf("verified candidate UUID changed on %s", nodeState.HostName)
		}
		publicationSQL := "EXCHANGE TABLES " + s.tablesClient.QualifiedTableName(tableState.DestinationTableName) + " AND " + s.tablesClient.QualifiedTableName(nodeState.CandidateTable.TableName)
		if nodeState.PreviousTableUUID == "" {
			publicationSQL = "RENAME TABLE " + s.tablesClient.QualifiedTableName(nodeState.CandidateTable.TableName) + " TO " + s.tablesClient.QualifiedTableName(tableState.DestinationTableName)
		}
		publicationCommand, err := s.tablesClient.PrepareSessionCommand(ctx, nodeState.HostName, publicationSQL)
		if err != nil {
			return xerrors.Errorf("publishTableOnNode: %w", err)
		}
		if err := s.recordCommand(ctx, nodeState.HostName, nil, publicationCommand); err != nil {
			return xerrors.Errorf("publishTableOnNode: %w", err)
		}
		// The persisted command remains until a subsequent, fenced UUID observation.
		// Never infer the outcome from just the response to EXCHANGE.
		if err := s.executeAndFenceCommand(ctx, nodeState.HostName, publicationCommand); err != nil {
			return xerrors.Errorf("publishTableOnNode: %w", err)
		}
	}
}

func (s *tableCommands) dropOwnedTemporaryTable(ctx context.Context, temporaryTable *ch_eo_common.OwnedTable) error {
	if err := s.fencePendingTableCommand(ctx, temporaryTable.ExpectedTableUUID); err != nil {
		return xerrors.Errorf("dropOwnedTemporaryTable: %w", err)
	}
	commandHostName, err := s.databaseOperations.resolveCommandHostName(ctx, temporaryTable.CommandHostName)
	if err != nil {
		return xerrors.Errorf("dropOwnedTemporaryTable: %w", err)
	}
	temporaryTable.CommandHostName = commandHostName

	actualTableUUID, err := s.tablesClient.TableUUID(ctx, temporaryTable.CommandHostName, temporaryTable.TableName)
	if err != nil {
		return xerrors.Errorf("dropOwnedTemporaryTable: %w", err)
	}
	if actualTableUUID == "" {
		return nil
	}
	if actualTableUUID != temporaryTable.ExpectedTableUUID {
		return xerrors.Errorf("refusing to drop %s/%s: UUID %s is not owned UUID %s", temporaryTable.CommandHostName, temporaryTable.TableName, actualTableUUID, temporaryTable.ExpectedTableUUID)
	}
	if err := s.executeOwnedTableCommand(ctx, temporaryTable, "DROP TABLE "+s.tablesClient.QualifiedTableName(temporaryTable.TableName)+" SYNC"); err != nil {
		return xerrors.Errorf("dropOwnedTemporaryTable: %w", err)
	}
	actualTableUUID, err = s.tablesClient.TableUUID(ctx, temporaryTable.CommandHostName, temporaryTable.TableName)
	if err != nil {
		return xerrors.Errorf("dropOwnedTemporaryTable: %w", err)
	}
	if actualTableUUID != "" {
		return xerrors.New("snapshot table still exists after DROP")
	}
	return nil
}

func (s *tableCommands) dropDiscoveredTemporaryTables(ctx context.Context, tableState *ch_eo_common.SnapshotTable) error {
	if tableState.TemporaryTablePrefix == "" {
		return xerrors.New("snapshot temporary table prefix is missing")
	}
	availableNodesByShard, err := s.databaseOperations.SelectNodesByShard(ctx)
	if err != nil {
		return xerrors.Errorf("dropDiscoveredTemporaryTables: %w", err)
	}
	for _, shardNodes := range availableNodesByShard {
		for _, node := range shardNodes {
			if err := s.dropTemporaryTablesOnNode(ctx, tableState, node); err != nil {
				return xerrors.Errorf("dropDiscoveredTemporaryTables: %w", err)
			}
		}
	}
	return nil
}

// Explicit server macros override the database's shard and replica macros.
// Verify the resulting table topology before importing any partitions.

func (s *tableCommands) validateTableReplicaTopology(ctx context.Context, tableName string) error {
	shardIndexByKeeperPath := make(map[string]int)
	configuredNodesByShard := s.tablesClient.NodesByShard()
	availableNodesByShard, err := s.databaseOperations.SelectNodesByShard(ctx)
	if err != nil {
		return xerrors.Errorf("validateTableReplicaTopology: %w", err)
	}
	for shardIndex, shardNodes := range availableNodesByShard {
		shardKeeperPath := ""
		seenReplicaNames := make(map[string]bool)
		for _, node := range shardNodes {
			if err := s.databaseOperations.SyncDatabaseDDLOnNode(ctx, node.Host.Name); err != nil {
				return xerrors.Errorf("validateTableReplicaTopology: %w", err)
			}

			var reportedReplicaCount uint32
			var keeperPath, replicaName string
			err := s.tablesClient.ReadRow(ctx, node.Host.Name, "SELECT total_replicas, zookeeper_path, replica_name FROM system.replicas WHERE database = ? AND table = ?", []any{s.databaseName, tableName}, &reportedReplicaCount, &keeperPath, &replicaName)
			if err != nil {
				return xerrors.Errorf("read candidate replicas on %s: %w", node.Host.Name, err)
			}
			if !s.databaseOperations.IsReplicaCountCompatibleWithTopology(int(reportedReplicaCount), len(configuredNodesByShard[shardIndex]), len(shardNodes)) || seenReplicaNames[replicaName] || (shardKeeperPath != "" && shardKeeperPath != keeperPath) {
				return xerrors.New("candidate shard/replica macros do not match the destination topology")
			}
			otherShardIndex, found := shardIndexByKeeperPath[keeperPath]
			if found && otherShardIndex != shardIndex {
				return xerrors.New("candidate shards share a Keeper path; check the shard macro")
			}
			shardKeeperPath = keeperPath
			shardIndexByKeeperPath[keeperPath] = shardIndex
			seenReplicaNames[replicaName] = true
		}
	}
	return nil
}

func (s *tableCommands) checkTableUUIDAndRowCounts(ctx context.Context, table *ch_eo_common.OwnedTable, expectedRowCountsByShard []uint64, syncTableReplicas bool) error {
	return ch_eo_common.RetryReplicaBarrier(
		ctx,
		func() error {
			return s.checkTableUUIDAndRowCountsOnce(ctx, table, expectedRowCountsByShard, syncTableReplicas)
		},
	)
}

// checkTableUUIDAndRowCountsOnce checks physical row counts on the replicas selected by the strategy.
func (s *tableCommands) checkTableUUIDAndRowCountsOnce(ctx context.Context, table *ch_eo_common.OwnedTable, expectedRowCountsByShard []uint64, syncTableReplicas bool) error {
	var replicaChecks errgroup.Group
	replicaChecks.SetLimit(32)
	availableNodesByShard, err := s.databaseOperations.SelectNodesByShard(ctx)
	if err != nil {
		return xerrors.Errorf("checkTableUUIDAndRowCountsOnce: %w", err)
	}
	for shardIndex, shardNodes := range availableNodesByShard {
		for _, node := range shardNodes {
			replicaChecks.Go(
				func() error {
					if err := s.databaseOperations.SyncDatabaseDDLOnNode(ctx, node.Host.Name); err != nil {
						return xerrors.Errorf("sync database DDL: %w", err)
					}

					if syncTableReplicas {
						if err := s.executeCommandOverHTTP(ctx, node.Host.Name, "SYSTEM SYNC REPLICA "+s.tablesClient.QualifiedTableName(table.TableName), nil); err != nil {
							return xerrors.Errorf("synchronize shared table: %w", err)
						}
					}
					return s.tablesClient.CheckTableUUIDAndRowCount(ctx, node.Host.Name, table.TableName, table.ExpectedTableUUID, expectedRowCountsByShard[shardIndex])
				},
			)
		}
	}
	return replicaChecks.Wait()
}

// Used both by a single shard retry and by recovery before discovering tables.
// Other shard workers may still be running; never fence or erase their commands.

func (s *tableCommands) fencePendingTableCommand(ctx context.Context, tableUUID string) error {
	pendingTable := s.pendingTable(tableUUID)
	if pendingTable == nil {
		return nil
	}
	if pendingTable.PendingCommand != nil {
		if err := s.fenceCommand(ctx, pendingTable.CommandHostName, pendingTable.PendingCommand); err != nil {
			return xerrors.Errorf("fence table command: %w", err)
		}
	}
	actualUUID, err := s.tablesClient.TableUUID(ctx, pendingTable.CommandHostName, pendingTable.TableName)
	if err != nil {
		return xerrors.Errorf("read pending table: %w", err)
	}
	if actualUUID != "" && actualUUID != pendingTable.ExpectedTableUUID {
		return xerrors.New("pending table UUID changed")
	}
	return s.recordCommand(ctx, pendingTable.CommandHostName, pendingTable, nil)
}

func (s *tableCommands) PrepareLoadingTables(ctx context.Context, partIDHash string) ([]ch_eo_common.OwnedTable, error) {
	tableState := s.table
	var partTables []ch_eo_common.OwnedTable
	// Never reuse an attempt's name or UUID: an earlier worker's delayed DDL
	// may still arrive through the shared session after reassignment.
	attemptKey := partIDHash + "_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	loadingTableName := tableState.TemporaryTablePrefix + attemptKey + "_loading"
	seenTableUUIDs := make(map[string]bool)
	var creates errgroup.Group
	creates.SetLimit(32)
	for _, shardNodes := range s.tablesClient.NodesByShard() {
		stagingHostName := s.databaseOperations.PartTableIdentityHostName(shardNodes[0].Host.Name)
		expectedUUID := ch_eo_common.PartTableUUID(tableState.TemporaryTablePrefix, attemptKey, stagingHostName)
		partTables = append(partTables, *ch_eo_common.NewOwnedTable(stagingHostName, loadingTableName, expectedUUID))
		if seenTableUUIDs[expectedUUID] {
			continue
		}
		seenTableUUIDs[expectedUUID] = true
		creates.Go(func() error {
			newStagingEngineSQL := s.databaseOperations.newStagingEngineSQL()
			loadingDDL, err := ch_eo_common.BuildCreateTableDDL(s.databaseName, loadingTableName, expectedUUID, tableState.DDLTemplate, newStagingEngineSQL)
			if err != nil {
				return xerrors.Errorf("build staging DDL: %w", err)
			}
			if err := s.executeStagingCommand(ctx, stagingHostName, loadingDDL); err != nil {
				return xerrors.Errorf("create loading part: %w", err)
			}
			return nil
		})
	}
	if err := creates.Wait(); err != nil {
		return nil, xerrors.Errorf("prepare loading tables: %w", err)
	}
	return partTables, nil
}

// WaitForStagingDDL waits for each staging session to become available and
// for the database-specific DDL barrier. It leaves the sessions open.
func (s *tableCommands) WaitForStagingDDL(ctx context.Context) error {
	for hostName := range s.table.StagingDDLSessionIDs {
		if err := s.executeStagingCommand(ctx, hostName, "SELECT 1 FORMAT Null"); err != nil {
			return xerrors.Errorf("wait for staging DDL: %w", err)
		}
	}
	return nil
}
func (s *tableCommands) RenamePartTable(ctx context.Context, table *ch_eo_common.OwnedTable, readyName string) error {
	return s.executeStagingCommand(ctx, table.CommandHostName, "RENAME TABLE "+s.tablesClient.QualifiedTableName(table.TableName)+" TO "+s.tablesClient.QualifiedTableName(readyName))
}
func (s *tableCommands) CloseStaging(ctx context.Context) error {
	return s.fenceStagingCommands(ctx, s.table)
}
func (s *tableCommands) WaitForPendingCommandsAndClearRecords(ctx context.Context) error {
	for _, tableUUID := range s.pendingTableUUIDs() {
		if err := s.fencePendingTableCommand(ctx, tableUUID); err != nil {
			return xerrors.Errorf("settle pending command: %w", err)
		}
	}
	return nil
}
func (s *tableCommands) DropTemporaryTables(ctx context.Context) error {
	return s.dropDiscoveredTemporaryTables(ctx, s.table)
}
func (s *tableCommands) DropPreviousTables(ctx context.Context) error {
	processedCandidateUUIDs := make(map[string]bool)
	for _, replica := range s.table.NodeStates {
		previousTable := replica.CandidateTable
		if processedCandidateUUIDs[previousTable.ExpectedTableUUID] {
			continue
		}
		processedCandidateUUIDs[previousTable.ExpectedTableUUID] = true
		if replica.PreviousTableUUID == "" {
			continue
		}
		previousTable.ExpectedTableUUID = replica.PreviousTableUUID
		if err := s.dropOwnedTemporaryTable(ctx, &previousTable); err != nil {
			return xerrors.Errorf("drop previous table: %w", err)
		}
	}
	return nil
}
func (s *tableCommands) PreparePreviousPublicationForCleanup(ctx context.Context) error {
	availableNodes, err := s.databaseOperations.SelectNodesByShard(ctx)
	if err != nil {
		return xerrors.Errorf("prepare previous publication cleanup: %w", err)
	}
	reachableHosts := make(map[string]bool)
	for _, shardNodes := range availableNodes {
		for _, node := range shardNodes {
			reachableHosts[node.Host.Name] = true
		}
	}
	for _, replica := range s.table.NodeStates {
		if command := replica.PendingPublicationCommand; command != nil {
			if err := s.fenceCommand(ctx, replica.HostName, command); err != nil {
				return xerrors.Errorf("prepare previous publication cleanup: %w", err)
			}
		}
		if !reachableHosts[replica.HostName] {
			continue
		}
		actualUUID, err := s.tablesClient.TableUUID(ctx, replica.HostName, s.table.DestinationTableName)
		if err != nil {
			return xerrors.Errorf("prepare previous publication cleanup: %w", err)
		}
		if actualUUID != "" && actualUUID != replica.PreviousTableUUID && actualUUID != replica.CandidateTable.ExpectedTableUUID {
			return xerrors.Errorf("unexpected destination UUID on %s/%s", replica.HostName, s.table.DestinationTableName)
		}
	}
	return nil
}

// recordCommand waits for the sink's durable checkpoint before changing the
// local view or executing SQL. Serialize checkpoints across parallel shard builds.
func (s *tableCommands) recordCommand(ctx context.Context, hostName string, ownedTable *ch_eo_common.OwnedTable, command *ch_eo_common.Command) error {
	s.checkpointMutex.Lock()
	defer s.checkpointMutex.Unlock()
	if s.checkpointError != nil {
		return xerrors.Errorf("command checkpoints blocked: %w", s.checkpointError)
	}
	if s.checkpoint == nil {
		return xerrors.New("SQL command requires a checkpoint handler")
	}
	if err := s.checkpoint(ctx, hostName, ownedTable, command); err != nil {
		s.checkpointError = err
		return xerrors.Errorf("checkpoint SQL command: %w", err)
	}
	if ownedTable != nil {
		if command == nil {
			delete(s.table.PendingTableCommands, ownedTable.ExpectedTableUUID)
		} else {
			if s.table.PendingTableCommands == nil {
				s.table.PendingTableCommands = make(map[string]*ch_eo_common.OwnedTable)
			}
			pendingTable := ch_eo_common.CopyOwnedTable(ownedTable)
			pendingTable.PendingCommand = ch_eo_common.CopyCommand(command)
			s.table.PendingTableCommands[ownedTable.ExpectedTableUUID] = pendingTable
		}
		return nil
	}
	for nodeIndex := range s.table.NodeStates {
		if s.table.NodeStates[nodeIndex].HostName == hostName {
			s.table.NodeStates[nodeIndex].PendingPublicationCommand = ch_eo_common.CopyCommand(command)
			return nil
		}
	}
	return xerrors.Errorf("publication host %s is absent from table data", hostName)
}

func (s *tableCommands) checkpointFailed() bool {
	s.checkpointMutex.Lock()
	defer s.checkpointMutex.Unlock()
	return s.checkpointError != nil
}

func (s *tableCommands) pendingTableUUIDs() []string {
	s.checkpointMutex.Lock()
	defer s.checkpointMutex.Unlock()
	result := make([]string, 0, len(s.table.PendingTableCommands))
	for tableUUID := range s.table.PendingTableCommands {
		result = append(result, tableUUID)
	}
	return result
}

func (s *tableCommands) pendingTable(tableUUID string) *ch_eo_common.OwnedTable {
	s.checkpointMutex.Lock()
	defer s.checkpointMutex.Unlock()
	return ch_eo_common.CopyOwnedTable(s.table.PendingTableCommands[tableUUID])
}

func (s *tableCommands) dropTemporaryTablesOnNode(ctx context.Context, tableState *ch_eo_common.SnapshotTable, node ch_eo_common.Node) error {
	discoveredTableNames, err := s.tablesClient.ReadStrings(ctx, node.Host.Name, "SELECT name FROM system.tables WHERE database = ? AND startsWith(name, ?)", s.databaseName, tableState.TemporaryTablePrefix)
	if err != nil {
		return xerrors.Errorf("dropDiscoveredTemporaryTables: %w", err)
	}
	for _, tableName := range discoveredTableNames {
		if tableName == tableState.DestinationTableName {
			return xerrors.New("temporary prefix overlaps destination table")
		}
		actualTableUUID, err := s.tablesClient.TableUUID(ctx, node.Host.Name, tableName)
		if err != nil {
			return xerrors.Errorf("dropDiscoveredTemporaryTables: %w", err)
		}
		if actualTableUUID == "" {
			continue
		}
		// Part names encode the inputs for a deterministic UUID; other
		// temporary names end with their UUID. EXCHANGE can replace it.
		if len(tableName) < 36 {
			return xerrors.New("invalid temporary table name")
		}
		expectedTableUUID := tableName[len(tableName)-36:]
		if attemptKey, isPartTable := ch_eo_common.PartAttemptKeyFromTableName(tableState.TemporaryTablePrefix, tableName); isPartTable {
			identityHostName := s.databaseOperations.PartTableIdentityHostName(node.Host.Name)
			expectedTableUUID = ch_eo_common.PartTableUUID(tableState.TemporaryTablePrefix, attemptKey, identityHostName)
		}
		if _, err := uuid.Parse(expectedTableUUID); err != nil {
			return xerrors.Errorf("invalid temporary table UUID in %s: %w", tableName, err)
		}
		if actualTableUUID != expectedTableUUID {
			isOwnedPreviousTable := false
			for _, nodeState := range tableState.NodeStates {
				if nodeState.HostName != node.Host.Name || nodeState.CandidateTable.TableName != tableName || nodeState.PreviousTableUUID != actualTableUUID {
					continue
				}
				destinationTableUUID, err := s.tablesClient.TableUUID(ctx, node.Host.Name, tableState.DestinationTableName)
				if err != nil {
					return xerrors.Errorf("check exchanged table: %w", err)
				}
				isOwnedPreviousTable = destinationTableUUID == nodeState.CandidateTable.ExpectedTableUUID
			}
			if !isOwnedPreviousTable {
				return xerrors.Errorf("refusing to drop %s: unexpected UUID %s", tableName, actualTableUUID)
			}
		}
		if tableState.PublicationDecided() {
			for _, nodeState := range tableState.NodeStates {
				if nodeState.HostName != node.Host.Name || nodeState.CandidateTable.TableName != tableName || nodeState.CandidateTable.ExpectedTableUUID != actualTableUUID {
					continue
				}
				// A replica can return after publication skipped it. Keep its
				// verified candidate until publication reaches this host too.
				destinationTableUUID, err := s.tablesClient.TableUUID(ctx, node.Host.Name, tableState.DestinationTableName)
				if err != nil {
					return xerrors.Errorf("check candidate publication before cleanup: %w", err)
				}
				if destinationTableUUID != nodeState.CandidateTable.ExpectedTableUUID {
					return xerrors.Errorf("refusing to drop unpublished snapshot candidate %s/%s: destination UUID %s is not candidate UUID %s", node.Host.Name, tableName, destinationTableUUID, nodeState.CandidateTable.ExpectedTableUUID)
				}
			}
		}
		temporaryTable := ch_eo_common.NewOwnedTable(node.Host.Name, tableName, actualTableUUID)
		if err := s.dropOwnedTemporaryTable(ctx, temporaryTable); err != nil {
			return xerrors.Errorf("dropDiscoveredTemporaryTables: %w", err)
		}
	}
	return nil
}
