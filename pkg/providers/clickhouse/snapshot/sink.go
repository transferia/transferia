package snapshot

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/blang/semver/v4"
	"github.com/google/uuid"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/conn"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/sharding"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/database_operations"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/part_uploader"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/transfer_state"
	"github.com/transferia/transferia/pkg/providers/clickhouse/topology"
	"github.com/transferia/transferia/pkg/providers/metrika/ddlbuilder"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/errgroup"
)

type Sink struct {
	logger                       log.Logger
	transferID                   string
	dstConfig                    *clickhouse_model.ChDestination
	tablesClient                 *ch_eo_common.Client
	mu                           sync.Mutex // used in: Push/Close/Commit - so, doesn't matter for business logic
	publicationStore             *transfer_state.State
	defaultReplicatedTableEngine bool

	// Loaded once for this registration sink; new entries follow durable RegisterStaging.
	sourceTableByDestinationName map[string]abstract.TableID

	databaseOperations       database_operations.DatabaseOperations
	destinationTableNameFunc func(abstract.ChangeItem) string          // determines table_name by changeItem
	destinationDDLFunc       func(abstract.ChangeItem) (string, error) // build 'DDL' from changeItem
	uploader                 *part_uploader.PartUploader
	closeHTTPClient          func() // nil for borrowed node clients; set for the sink's own transport
	uploadContext            context.Context
	cancelUpload             context.CancelFunc
	closed                   bool
	poison                   error
}

var _ abstract.Committable = (*Sink)(nil)

// newSink is separate from NewSinkFromTransfer so tests can supply their own
// connections, uploader and transfer state client without resolving topology
// or opening new connections.
func newSink(
	ctx context.Context,
	logger log.Logger,
	dstConfig *clickhouse_model.ChDestination,
	transferStateClient coordinator.TransferState,
	transferID string,
	operationID string,
	nodesByShard [][]ch_eo_common.Node,
	defaultReplicatedTableEngine bool,
	destinationTableNameFunc func(abstract.ChangeItem) string,
	destinationDDLFunc func(abstract.ChangeItem) (string, error),
	uploader *part_uploader.PartUploader,
) (*Sink, error) {
	if dstConfig == nil || transferStateClient == nil || transferID == "" || dstConfig.Database == "" || len(nodesByShard) == 0 || uploader == nil {
		return nil, xerrors.New("incomplete ClickHouse snapshot configuration")
	}
	seenHosts := make(map[string]bool)
	for _, shardNodes := range nodesByShard {
		if len(shardNodes) == 0 {
			return nil, xerrors.New("empty ClickHouse shard")
		}
		for _, clickhouseNode := range shardNodes {
			if clickhouseNode.Host == nil || clickhouseNode.DB == nil || clickhouseNode.HTTP == nil || seenHosts[clickhouseNode.Host.Name] {
				return nil, xerrors.New("snapshot topology requires unique, fully configured hosts")
			}
			seenHosts[clickhouseNode.Host.Name] = true
		}
	}
	switch dstConfig.Cleanup {
	case model.Drop, model.Truncate, model.DisabledCleanup:
	default:
		return nil, xerrors.Errorf("unsupported snapshot cleanup policy %q", dstConfig.Cleanup)
	}
	uploadContext, cancelUpload := context.WithCancel(ctx)
	sink := &Sink{
		logger:                       logger,
		transferID:                   transferID,
		dstConfig:                    dstConfig,
		tablesClient:                 nil,
		mu:                           sync.Mutex{},
		publicationStore:             transfer_state.NewState(transferStateClient, transferID, operationID),
		defaultReplicatedTableEngine: defaultReplicatedTableEngine,
		sourceTableByDestinationName: nil,
		databaseOperations:           nil,
		destinationTableNameFunc:     destinationTableNameFunc,
		destinationDDLFunc:           destinationDDLFunc,
		uploader:                     uploader,
		closeHTTPClient:              nil,
		uploadContext:                uploadContext,
		cancelUpload:                 cancelUpload,
		closed:                       false,
		poison:                       nil,
	}
	sink.tablesClient = ch_eo_common.NewClient(dstConfig, logger, nodesByShard)
	return sink, nil
}

// NewSinkFromTransfer resolves the destination and owns its connections. The adapter
// supplies only naming and DDL generation from the ordinary ClickHouse sink.
func NewSinkFromTransfer(
	ctx context.Context,
	logger log.Logger,
	registry core_metrics.Registry,
	transferStateClient coordinator.TransferState,
	transfer *model.Transfer,
	operationID string,
	cleanupOnly bool,
	makeTableNameAndDDLBuilders func(clickhouse_model.ChSinkParams, semver.Version) (func(abstract.ChangeItem) string, func(abstract.ChangeItem) (string, error)),
) (*Sink, error) {
	dstConfig, err := ch_eo_common.ValidateSnapshotConfiguration(transfer, operationID, cleanupOnly)
	if err != nil {
		return nil, xerrors.Errorf("new snapshot sink: %w", err)
	}
	sinkParams, err := dstConfig.ToSinkParams(transfer)
	if err != nil {
		return nil, xerrors.Errorf("new snapshot sink: %w", err)
	}
	resolvedTopology, err := topology.ResolveTopology(sinkParams, logger)
	if err != nil {
		return nil, xerrors.Errorf("new snapshot sink: %w", err)
	}
	httpClient, err := httpclient.NewHTTPClientImpl(sinkParams)
	if err != nil {
		return nil, xerrors.Errorf("new snapshot sink: %w", err)
	}
	shardNames := make([]string, 0, len(sinkParams.Shards()))
	for shardName := range sinkParams.Shards() {
		shardNames = append(shardNames, shardName)
	}
	sort.Strings(shardNames)
	sharder := sharding.CHSharder(sinkParams, transfer.ID)
	if sinkParams.ShardByTransferID() {
		// Other transfers may own the same table on the remaining shards.
		// Keep both snapshot data and publication on this transfer's shard.
		var emptyRow abstract.ChangeItem
		shardNames = []string{shardNames[sharder(emptyRow)]}
		sharder = sharding.ConstSharder()
	}
	nodesByShard := make([][]ch_eo_common.Node, len(shardNames))
	uploader := part_uploader.NewUploader(len(shardNames), sinkParams.Database(), sinkParams.IsUpdateable(), stats.NewChStats(registry), sharder)
	constructionComplete := false
	defer func() {
		if constructionComplete {
			return
		}
		_ = uploader.Close()
		for _, shardNodes := range nodesByShard {
			for _, node := range shardNodes {
				_ = node.DB.Close()
			}
		}
		httpClient.CloseIdleConnections()
	}()

	var serverVersionForDDL semver.Version
	for shardIndex, shardName := range shardNames {
		shardHosts := append(sinkParams.Shards()[shardName][:0:0], sinkParams.Shards()[shardName]...)
		sort.Slice(
			shardHosts,
			func(leftIndex int, rightIndex int) bool {
				return shardHosts[leftIndex].Name < shardHosts[rightIndex].Name
			},
		)
		for hostIndex, hostName := range shardHosts {
			serverParams := sinkParams.MakeChildShardParams(shardHosts).MakeChildServerParams(hostName)
			metadataConnectionOptions, err := conn.GetClickhouseOptions(serverParams, shardHosts[:0:0])
			if err != nil {
				return nil, xerrors.Errorf("new snapshot sink: %w", err)
			}
			metadataConnectionOptions.Addr = []string{fmt.Sprintf("%s:%d", hostName.Name, hostName.NativePort)}
			metadataConnectionOptions.Settings = clickhouse_go.Settings{"insert_deduplicate": 0}
			node := ch_eo_common.NewNode(hostName, clickhouse_go.OpenDB(metadataConnectionOptions), httpClient)
			nodesByShard[shardIndex] = append(nodesByShard[shardIndex], node)
			// Metadata connections stay lazy: offline non-writer replicas must
			// not prevent constructing a Replicated-database sink.
			if hostIndex != 0 || cleanupOnly {
				continue
			}
			serverVersion, err := ch_eo_common.ReadSnapshotServerVersion(node.DB)
			if err != nil {
				return nil, xerrors.Errorf("new snapshot sink: %w", err)
			}
			if shardIndex == 0 {
				serverVersionForDDL = serverVersion
			}
			// Older servers have synchronous INSERTs only and reject async_insert.
			// Disable server-side buffering when this setting exists: the uploader
			// must receive the actual INSERT result before marking a part ready.
			var supportsAsyncInsert uint64
			settingsContext, cancelSettingsRead := context.WithTimeout(context.Background(), 30*time.Second)
			err = node.DB.QueryRowContext(settingsContext, "SELECT count() FROM system.settings WHERE name = 'async_insert'").Scan(&supportsAsyncInsert)
			cancelSettingsRead()
			if err != nil {
				return nil, xerrors.Errorf("check snapshot INSERT settings: %w", err)
			}
			if supportsAsyncInsert != 0 {
				metadataConnectionOptions.Settings["async_insert"] = 0
			}
			nativeConnection, err := part_uploader.OpenConnection(metadataConnectionOptions)
			if err != nil {
				return nil, xerrors.Errorf("open snapshot native writer: %w", err)
			}
			// A snapshot block can span many partitions. Lift the limit before
			// sending rows: retrying a failed INSERT could write them twice.
			insertSettings := sinkParams.InsertSettings()
			insertSettings.UnlimitedPartitionsPerInsertBlock = true
			if err := uploader.AddShard(shardIndex, nativeConnection, insertSettings.ToQueryOption(serverVersion)); err != nil {
				_ = nativeConnection.Close()
				return nil, xerrors.Errorf("configure snapshot shard writer: %w", err)
			}
		}
	}
	destinationTableName, destinationDDL := makeTableNameAndDDLBuilders(sinkParams, serverVersionForDDL)
	sink, err := newSink(
		ctx,
		logger,
		dstConfig,
		transferStateClient,
		transfer.ID,
		operationID,
		nodesByShard,
		resolvedTopology.ClusterName() != "",
		destinationTableName,
		destinationDDL,
		uploader,
	)
	if err != nil {
		return nil, xerrors.Errorf("new snapshot sink: %w", err)
	}
	if !cleanupOnly && operationID != "" {
		sink.tablesClient.SetReplicaFailures(stagingDDLSessions.replicaFailures(sink.stagingDDLOperationKey(operationID)))
	}
	sink.closeHTTPClient = httpClient.CloseIdleConnections
	constructionComplete = true
	return sink, nil
}

func (s *Sink) Push(items []abstract.ChangeItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return xerrors.New("snapshot sink is closed")
	}
	// early exit
	// The outer sink middleware may retry a failed Push. Poisoning this sink
	// makes that retry harmless; only a fresh whole-part attempt can proceed.
	if s.poison != nil {
		//nolint:descriptiveerrors
		return s.poison
	}

	// normal processing
	err := s.processItems(s.uploadContext, items)
	if err != nil {
		uploadError := abstract.NewTableUploadError(err)
		s.poison = uploadError
		//nolint:descriptiveerrors
		return uploadError
	}
	return nil
}

func (s *Sink) processItems(ctx context.Context, items []abstract.ChangeItem) error {
	batchBegin := 0
	for {
		if batchBegin >= len(items) {
			break
		}
		item := items[batchBegin]
		if item.Kind == abstract.InsertKind {
			batchEnd := ch_eo_common.FindTablePartInsertBatchEnd(items, batchBegin)
			if err := s.uploader.Write(items[batchBegin:batchEnd]); err != nil {
				return xerrors.Errorf("processItems: %w", err)
			}
			batchBegin = batchEnd
			continue
		}
		switch item.Kind {
		case abstract.InitShardedTableLoad, abstract.DropTableKind, abstract.TruncateTableKind:
			if item.Kind != abstract.InitShardedTableLoad && item.TableSchema == nil {
				// UploadTables sends cleanup intent before reading the source schema.
				// Register on InitShardedTableLoad instead; Commit applies the cleanup.
				break
			}
			if err := s.initTableState(ctx, item); err != nil {
				s.stopStagingDDLKeepalives()
				return xerrors.Errorf("processItems: %w", err)
			}
		case abstract.ClickhouseDDLBuilderKind:
			if err := s.checkMetrikaDDL(ctx, item); err != nil {
				return xerrors.Errorf("check Metrika DDL: %w", err)
			}
		case abstract.InitTableLoad: // begin of tablePartID
			if err := s.initPartUpload(ctx, item); err != nil {
				return xerrors.Errorf("processItems: %w", err)
			}
		case abstract.DoneTableLoad: // end of tablePartID
			if err := s.uploader.Finish(ctx, item.TablePartID()); err != nil {
				return xerrors.Errorf("processItems: %w", err)
			}
		case abstract.DoneShardedTableLoad:
			s.stopStagingDDLKeepalives()
		case abstract.SynchronizeKind:
			// Cleanup is intent; the live target is only changed by Commit.
		default:
			return xerrors.Errorf("unsupported event %q in exactly-once snapshot", item.Kind)
		}
		batchBegin++
	}
	return nil
}

func (s *Sink) buildTemporaryTablePrefix(ctx context.Context, sourceTableID abstract.TableID) (string, error) {
	maxTableNameLength := ch_eo_common.LegacyMaximumTableNameLength(s.dstConfig.Database)
	selectNodesByShard, err := s.databaseOperations.SelectNodesByShard(ctx)
	if err != nil {
		return "", err
	}
	var limitChecks errgroup.Group
	limitChecks.SetLimit(32)
	var limitMutex sync.Mutex
	for _, shardNodes := range selectNodesByShard {
		for _, node := range shardNodes {
			limitChecks.Go(func() error {
				var hostTableNameLimit uint64
				err := s.tablesClient.ReadRow(ctx, node.Host.Name, "SELECT getMaxTableNameLengthForDatabase(?)", []any{s.dstConfig.Database}, &hostTableNameLimit)
				if err != nil {
					var serverError *clickhouse_go.Exception
					if !xerrors.As(err, &serverError) || serverError.Code != 46 {
						return xerrors.Errorf("read temporary table name limit on %s: %w", node.Host.Name, err)
					}
					hostTableNameLimit = ch_eo_common.LegacyMaximumTableNameLength(s.dstConfig.Database)
				}
				limitMutex.Lock()
				maxTableNameLength = min(maxTableNameLength, hostTableNameLimit)
				limitMutex.Unlock()
				return nil
			})
		}
	}
	if err := limitChecks.Wait(); err != nil {
		return "", xerrors.Errorf("read temporary table name limits: %w", err)
	}
	tableNamePrefix, err := ch_eo_common.SnapshotTemporaryTablePrefix(s.transferID, s.publicationStore.OperationID(), sourceTableID, maxTableNameLength)
	if err != nil {
		return "", abstract.NewFatalError(err)
	}
	return tableNamePrefix, nil
}

func (s *Sink) initPartUpload(ctx context.Context, item abstract.ChangeItem) error {
	tablePartID := item.TablePartID()
	table, err := s.publicationStore.LoadTable(ctx, s.publicationStore.TableStateKey(tablePartID.TableID))
	if err != nil {
		return xerrors.Errorf("read snapshot table before InitTableLoad: %w", err)
	}
	if table == nil {
		return xerrors.Errorf("snapshot table %s was not initialized before InitTableLoad", tablePartID.TableID.String())
	}
	if err := s.initDatabaseOperations(ctx); err != nil {
		return xerrors.Errorf("initPartUpload: %w", err)
	}
	if err := s.uploader.Start(ctx, table.Snapshot(), tablePartID.PartID, s.tablesClient, s.operationsForTable(table)); err != nil {
		return xerrors.Errorf("start snapshot part upload: %w", err)
	}
	return nil
}

// Commit allows runtime teardown to hand an unfinished durable publish
// decision to cleanup. Cancellation never rolls back or erases that decision.
func (s *Sink) Commit(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stopStagingDDLKeepalives()

	if s.closed || s.poison != nil {
		return xerrors.Errorf("snapshot is not ready to commit: closed=%t, error=%v", s.closed, s.poison)
	}
	if err := ctx.Err(); err != nil {
		return xerrors.Errorf("commit: %w", err)
	}

	tableStateKeys, err := s.publicationStore.ListStateKeysWithPrefix(ctx, s.publicationStore.OperationStatePrefix()+"table/")
	if err != nil {
		return xerrors.Errorf("read snapshot tables: %w", err)
	}
	if len(tableStateKeys) == 0 {
		return nil
	}
	if err := s.initDatabaseOperations(ctx); err != nil {
		return xerrors.Errorf("commit databaseOperations: %w", err)
	}
	tableStates := make([]*transfer_state.TableState, 0, len(tableStateKeys))
	readyPartsByTable := make(map[abstract.TableID][]ch_eo_common.ReadyPart)
	for _, tableStateKey := range tableStateKeys {
		tableState, err := s.publicationStore.LoadTable(ctx, tableStateKey)
		if err != nil {
			return xerrors.Errorf("commit: %w", err)
		}
		if tableState == nil {
			return xerrors.New("snapshot table disappeared")
		}
		if tableState.PublicationDecided() {
			// The verified candidate is durable. Its engine may have merged rows
			// since then, and publication must resume without rebuilding it.
			tableStates = append(tableStates, tableState)
			continue
		}
		if err := s.closeTableStaging(ctx, tableState); err != nil {
			return xerrors.Errorf("commit: %w", err)
		}
		readyParts, err := part_uploader.DiscoverReady(ctx, s.tablesClient, s.operationsForTable(tableState), s.dstConfig.Database, tableState.Snapshot())
		if err != nil {
			return xerrors.Errorf("commit: %w", err)
		}
		readyPartsByTable[tableState.SourceTableID()] = readyParts
		tableStates = append(tableStates, tableState)
	}

	// Validate every destination name before publishing any table.
	sourceTableByDestinationName := make(map[string]abstract.TableID)
	for _, tableState := range tableStates {
		if otherSourceTableID, ok := sourceTableByDestinationName[tableState.DestinationTableName()]; ok && otherSourceTableID != tableState.SourceTableID() {
			return xerrors.Errorf("snapshot source tables map to the same destination %s", tableState.DestinationTableName())
		}
		sourceTableByDestinationName[tableState.DestinationTableName()] = tableState.SourceTableID()
	}

	// Build and publish one table at a time using the already fenced records.
	for tableIndex := range tableStates {
		tableState := tableStates[tableIndex]
		if !tableState.PublicationDecided() {
			if err := s.buildCandidateTable(ctx, tableState, readyPartsByTable[tableState.SourceTableID()]); err != nil {
				return xerrors.Errorf("commit: %w", err)
			}
			// No further source work can fail after this point. The durable decision
			// must be completed by this process or a cleanup successor.
			if err := tableState.DecidePublication(ctx); err != nil {
				return xerrors.Errorf("commit: %w", err)
			}
		}
		if err := s.publishAndCleanupTable(ctx, tableState); err != nil {
			return xerrors.Errorf("commit: %w", err)
		}
	}
	return s.Cleanup(ctx)
}

func (s *Sink) Close() error {
	// A staging request may hold mu. Cancel it before waiting for Push to stop.
	s.cancelUpload()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	firstCloseError := s.uploader.Close()
	for _, shardNodes := range s.tablesClient.NodesByShard() {
		for _, clickhouseNode := range shardNodes {
			if err := clickhouseNode.DB.Close(); err != nil && firstCloseError == nil {
				firstCloseError = err
			}
		}
	}
	if s.closeHTTPClient != nil {
		s.closeHTTPClient()
	}
	return firstCloseError
}

// initTableState reuses an existing TableState. For a new table, it validates the
// destination, initializes staging DDL sessions, and saves the state in TransferState.
func (s *Sink) initTableState(ctx context.Context, item abstract.ChangeItem) error {
	if err := s.initDatabaseOperations(ctx); err != nil {
		return xerrors.Errorf("initTableState: %w", err)
	}
	existingTableState, err := s.publicationStore.LoadTable(ctx, s.publicationStore.TableStateKey(item.TableID()))
	if err != nil {
		return xerrors.Errorf("initTableState: %w", err)
	}
	if existingTableState != nil {
		if existingTableState.PublicationDecided() || existingTableState.PublicationComplete() {
			return xerrors.New("snapshot table has already been closed")
		}
		for hostName, sessionID := range existingTableState.Snapshot().StagingDDLSessionIDs {
			if err := s.startStagingDDLKeepalive(hostName, sessionID); err != nil {
				return xerrors.Errorf("resume staging DDL keepalive: %w", err)
			}
		}
		return nil
	}
	tableState, err := s.prepareTableState(ctx, item)
	if err != nil {
		return xerrors.Errorf("prepare snapshot table: %w", err)
	}
	temporaryTablePrefix, err := s.buildTemporaryTablePrefix(ctx, item.TableID())
	if err != nil {
		return xerrors.Errorf("prepare temporary table names: %w", err)
	}
	stagingSessions := make(map[string]string)
	var sessions errgroup.Group
	sessions.SetLimit(32)
	var sessionMutex sync.Mutex
	for _, shardNodes := range s.tablesClient.NodesByShard() {
		sessions.Go(func() error {
			hostName := shardNodes[0].Host.Name
			sessionID := uuid.NewString()
			// A failed initialization cannot mutate data: no worker has this ID yet.
			if err := s.tablesClient.ExecuteCommandOverHTTP(ctx, hostName, "SELECT 1 FORMAT Null", url.Values{"session_id": {sessionID}, "session_timeout": {"60"}}); err != nil {
				return xerrors.Errorf("initialize staging DDL session: %w", err)
			}
			sessionMutex.Lock()
			stagingSessions[hostName] = sessionID
			// Keep early hosts alive while slower hosts and the state save complete.
			keepaliveError := s.startStagingDDLKeepalive(hostName, sessionID)
			sessionMutex.Unlock()
			if keepaliveError != nil {
				return xerrors.Errorf("start staging DDL keepalive: %w", keepaliveError)
			}
			return nil
		})
	}
	if err := sessions.Wait(); err != nil {
		return xerrors.Errorf("initialize staging DDL sessions: %w", err)
	}
	if err := tableState.RegisterStaging(ctx, temporaryTablePrefix, stagingSessions); err != nil {
		return xerrors.Errorf("register snapshot table: %w", err)
	}
	s.sourceTableByDestinationName[tableState.DestinationTableName()] = tableState.SourceTableID()
	return nil
}

// loadDestinationTableNames restores registration checks once per sink, including
// after a restart. Commit independently checks the durable records before publication.
func (s *Sink) loadDestinationTableNames(ctx context.Context) error {
	if s.sourceTableByDestinationName != nil {
		return nil
	}
	tableStateKeysByOperation, err := s.publicationStore.ListTableStateKeysByOperation(ctx)
	if err != nil {
		return xerrors.Errorf("read registered snapshot tables: %w", err)
	}
	for operationID := range tableStateKeysByOperation {
		if operationID != s.publicationStore.OperationID() {
			return xerrors.New("previous ClickHouse snapshot still requires cleanup")
		}
	}
	sourceTableByDestinationName := make(map[string]abstract.TableID)
	for _, tableStateKey := range tableStateKeysByOperation[s.publicationStore.OperationID()] {
		tableState, err := s.publicationStore.LoadTable(ctx, tableStateKey)
		if err != nil {
			return xerrors.Errorf("read target mapping: %w", err)
		}
		if tableState == nil {
			continue
		}
		destinationTableName := tableState.DestinationTableName()
		if otherSourceTableID, ok := sourceTableByDestinationName[destinationTableName]; ok && otherSourceTableID != tableState.SourceTableID() {
			return xerrors.Errorf("snapshot source tables %s and %s map to the same destination %s", otherSourceTableID.String(), tableState.SourceTableID().String(), destinationTableName)
		}
		sourceTableByDestinationName[destinationTableName] = tableState.SourceTableID()
	}
	s.sourceTableByDestinationName = sourceTableByDestinationName
	return nil
}

func (s *Sink) prepareTableState(ctx context.Context, item abstract.ChangeItem) (*transfer_state.TableState, error) {
	nodesByShard := s.tablesClient.NodesByShard()
	if s.publicationStore.OperationID() == "" {
		return nil, xerrors.New("snapshot requires an operation ID")
	}
	sourceTableID := item.TableID()
	if err := s.loadDestinationTableNames(ctx); err != nil {
		return nil, xerrors.Errorf("prepareTableState: %w", err)
	}
	generatedTableDDL, err := s.snapshotDDL(item)
	if err != nil {
		return nil, xerrors.Errorf("prepareTableState: %w", err)
	}
	preparedTableState := s.publicationStore.NewTable(sourceTableID, s.destinationTableNameFunc(item), generatedTableDDL, s.defaultReplicatedTableEngine)
	if otherSourceTableID, ok := s.sourceTableByDestinationName[preparedTableState.DestinationTableName()]; ok && otherSourceTableID != sourceTableID {
		return nil, xerrors.Errorf("snapshot source tables %s and %s map to the same destination %s", otherSourceTableID.String(), sourceTableID.String(), preparedTableState.DestinationTableName())
	}
	selectNodesByShard, err := s.databaseOperations.SelectNodesByShard(ctx)
	if err != nil {
		return nil, xerrors.Errorf("prepareTableState: %w", err)
	}
	validatedTables := make([][]*transfer_state.TableState, len(selectNodesByShard))
	keeperPathsByShard := make([][]string, len(selectNodesByShard))
	replicaNamesByShard := make([][]string, len(selectNodesByShard))
	var nodeChecks errgroup.Group
	nodeChecks.SetLimit(32)
	for shardIndex, shardNodes := range selectNodesByShard {
		validatedTables[shardIndex] = make([]*transfer_state.TableState, len(shardNodes))
		keeperPathsByShard[shardIndex] = make([]string, len(shardNodes))
		replicaNamesByShard[shardIndex] = make([]string, len(shardNodes))
		for nodeIndex, clickhouseNode := range shardNodes {
			nodeChecks.Go(func() error {
				validatedTable := s.publicationStore.NewTable(sourceTableID, s.destinationTableNameFunc(item), generatedTableDDL, s.defaultReplicatedTableEngine)

				// check clickhouse version
				var serverVersionText, databaseEngine string
				if err := s.tablesClient.ReadRow(ctx, clickhouseNode.Host.Name, "SELECT version()", nil, &serverVersionText); err != nil {
					return xerrors.Errorf("prepareTableState: %w", err)
				}
				serverVersionParts := strings.Split(serverVersionText, ".")
				if len(serverVersionParts) > 3 {
					serverVersionParts = serverVersionParts[:3]
				}
				parsedServerVersion, err := semver.ParseTolerant(strings.Join(serverVersionParts, "."))
				if err != nil || parsedServerVersion.LT(semver.MustParse("21.3.7")) {
					return xerrors.Errorf("exactly-once snapshots require ClickHouse 21.3.7 or newer, found %s", serverVersionText)
				}

				// Both supported database engines provide table UUIDs and EXCHANGE TABLES.
				if err := s.tablesClient.ReadRow(ctx, clickhouseNode.Host.Name, "SELECT engine FROM system.databases WHERE name = ?", []any{s.dstConfig.Database}, &databaseEngine); err != nil {
					return xerrors.Errorf("prepareTableState: %w", err)
				}
				if databaseEngine != "Atomic" && databaseEngine != "Replicated" {
					return xerrors.Errorf("exactly-once snapshots require Atomic or Replicated database, found %s", databaseEngine)
				}
				// A returned replica may still have a previous snapshot's EXCHANGE
				// queued. Capture the live UUID only after that node has caught up.
				if err := s.databaseOperations.SyncDatabaseDDLOnNode(ctx, clickhouseNode.Host.Name); err != nil {
					return xerrors.Errorf("synchronize database before target registration: %w", err)
				}

				// check actual capabilities before registering any temporary table.
				err = s.tablesClient.CheckSessionFencingSupport(ctx, clickhouseNode.Host.Name)
				if err != nil {
					return xerrors.Errorf("unable to verify SQL session fencing on %s: %w", clickhouseNode.Host.Name, err)
				}

				previousTableUUID, err := s.tablesClient.TableUUID(ctx, clickhouseNode.Host.Name, validatedTable.DestinationTableName())
				if err != nil {
					return xerrors.Errorf("prepareTableState: %w", err)
				}
				if previousTableUUID != "" {
					// Read the schema after mutations finish: they may change column types.
					if err := s.tablesClient.WaitForMutations(ctx, clickhouseNode, validatedTable.DestinationTableName()); err != nil {
						return xerrors.Errorf("prepareTableState: %w", err)
					}
					// if table exists - check it's ENGINE
					var existingTableDDL, existingEngineName string
					var dependentTableCount uint64
					if err := s.tablesClient.ReadRow(ctx, clickhouseNode.Host.Name, "SELECT create_table_query, engine, toUInt64(length(dependencies_table)) FROM system.tables WHERE database = ? AND name = ?", []any{s.dstConfig.Database, validatedTable.DestinationTableName()}, &existingTableDDL, &existingEngineName, &dependentTableCount); err != nil {
						return xerrors.Errorf("prepareTableState: %w", err)
					}
					switch existingEngineName {
					case "MergeTree", "ReplacingMergeTree", "ReplicatedMergeTree", "ReplicatedReplacingMergeTree",
						"VersionedCollapsingMergeTree", "ReplicatedVersionedCollapsingMergeTree":
					default:
						return xerrors.Errorf("unsupported target engine %s", existingEngineName)
					}
					if strings.HasPrefix(existingEngineName, "Replicated") {
						// for 'ReplicatedMergeTree' this command waits for this replica to catch up with replication.
						if err := s.tablesClient.ExecuteCommandOverHTTP(ctx, clickhouseNode.Host.Name, "SYSTEM SYNC REPLICA "+s.tablesClient.QualifiedTableName(validatedTable.DestinationTableName()), nil); err != nil {
							return xerrors.Errorf("synchronize old target: %w", err)
						}

						// Reads FOR THIS TABLE:
						//   - count of replicas
						//   - keeper_path
						//   - replica name for this table
						// to verify that the configured hosts cover all replicas of the same replicated table.
						var replicaCount uint32
						var replicaKeeperPath, replicaName string
						if err := s.tablesClient.ReadRow(ctx, clickhouseNode.Host.Name, "SELECT total_replicas, zookeeper_path, replica_name FROM system.replicas WHERE database = ? AND table = ?", []any{s.dstConfig.Database, validatedTable.DestinationTableName()}, &replicaCount, &replicaKeeperPath, &replicaName); err != nil {
							return xerrors.Errorf("read target replica count: %w", err)
						}
						if !s.databaseOperations.IsReplicaCountCompatibleWithTopology(int(replicaCount), len(nodesByShard[shardIndex]), len(shardNodes)) {
							return xerrors.New("configured hosts do not cover all target replicas")
						}
						keeperPathsByShard[shardIndex][nodeIndex] = replicaKeeperPath
						replicaNamesByShard[shardIndex][nodeIndex] = replicaName
						if s.dstConfig.Cleanup == model.Drop {
							validatedTable.UseDefinition(validatedTable.DDLTemplate(), true)
						}
					}
					if dependentTableCount != 0 {
						return xerrors.New("target has unsupported dependencies")
					}
					if err := ch_eo_common.ValidateSnapshotTableDDL(existingTableDDL); err != nil {
						return xerrors.Errorf("validate snapshot target: %w", err)
					}
					if s.dstConfig.Cleanup != model.Drop {
						_, existingEngineExpression, _, err := ch_eo_common.ExtractTableDefinitionParts(existingTableDDL)
						if err != nil {
							return xerrors.Errorf("prepareTableState: %w", err)
						}
						nonReplicatedEngine, err := ch_eo_common.MakeEngineNonReplicated(existingEngineExpression)
						if err != nil {
							return xerrors.Errorf("prepareTableState: %w", err)
						}
						normalizedTableDDL, err := ch_eo_common.BuildCreateTableDDL("", "", "", existingTableDDL, nonReplicatedEngine)
						if err != nil {
							return xerrors.Errorf("prepareTableState: %w", err)
						}
						validatedTable.UseDefinition(normalizedTableDDL, strings.HasPrefix(existingEngineName, "Replicated"))
					}
				}
				validatedTable.AddReplica(clickhouseNode.Host.Name, shardIndex, previousTableUUID)
				validatedTables[shardIndex][nodeIndex] = validatedTable
				return nil
			})
		}
	}
	if err := nodeChecks.Wait(); err != nil {
		return nil, xerrors.Errorf("validate destination nodes: %w", err)
	}
	shardIndexByKeeperPath := make(map[string]int)
	for shardIndex, shardTables := range validatedTables {
		shardKeeperPath := ""
		seenReplicaNames := make(map[string]bool)
		for nodeIndex, validatedTable := range shardTables {
			node := validatedTable.Replicas()[0]
			previousReplicas := preparedTableState.Replicas()
			if len(previousReplicas) != 0 && (node.PreviousTableUUID == "") != (previousReplicas[0].PreviousTableUUID == "") {
				return nil, xerrors.New("target table exists on only some destination nodes")
			}
			keeperPath := keeperPathsByShard[shardIndex][nodeIndex]
			if keeperPath != "" {
				replicaName := replicaNamesByShard[shardIndex][nodeIndex]
				if shardKeeperPath != "" && shardKeeperPath != keeperPath {
					return nil, xerrors.New("configured replicas belong to different Keeper paths")
				}
				if otherShardIndex, found := shardIndexByKeeperPath[keeperPath]; found && otherShardIndex != shardIndex {
					return nil, xerrors.New("destination shards share the same replicated table")
				}
				if seenReplicaNames[replicaName] {
					return nil, xerrors.New("destination hosts address the same replica twice")
				}
				shardKeeperPath, shardIndexByKeeperPath[keeperPath], seenReplicaNames[replicaName] = keeperPath, shardIndex, true
			}
			if s.dstConfig.Cleanup == model.Drop {
				if validatedTable.UseReplicatedTableEngine() {
					preparedTableState.UseDefinition(preparedTableState.DDLTemplate(), true)
				}
			} else if len(previousReplicas) == 0 {
				preparedTableState.UseDefinition(validatedTable.DDLTemplate(), validatedTable.UseReplicatedTableEngine())
			} else if preparedTableState.DDLTemplate() != validatedTable.DDLTemplate() || preparedTableState.UseReplicatedTableEngine() != validatedTable.UseReplicatedTableEngine() {
				return nil, xerrors.New("destination replicas have different schemas")
			}
			preparedTableState.AddReplica(node.HostName, shardIndex, node.PreviousTableUUID)
		}
	}
	for _, nodeState := range s.databaseOperations.MissingNodeStates(preparedTableState.Snapshot()) {
		preparedTableState.AddReplica(nodeState.HostName, nodeState.ShardIndex, nodeState.PreviousTableUUID)
	}

	_, _, _, err = ch_eo_common.ExtractTableDefinitionParts(preparedTableState.DDLTemplate())
	if err != nil {
		return nil, xerrors.Errorf("prepareTableState: %w", err)
	}
	if s.dstConfig.Cleanup == model.Drop || preparedTableState.Replicas()[0].PreviousTableUUID == "" {
		for _, shardNodes := range nodesByShard {
			if len(shardNodes) > 1 {
				preparedTableState.UseDefinition(preparedTableState.DDLTemplate(), true)
			}
		}
	}
	for _, shardNodes := range nodesByShard {
		if len(shardNodes) > 1 && !preparedTableState.UseReplicatedTableEngine() {
			return nil, xerrors.New("multiple replicas require a ReplicatedMergeTree target")
		}
	}
	return preparedTableState, nil
}

// publishAndCleanupTableOnce reconciles publication by UUID before removing
// the previous generation. Atomic follows its replica availability mode; Replicated requires
// every reachable node and queues DDL for unavailable replicas.
func (s *Sink) publishAndCleanupTableOnce(ctx context.Context, tableState *transfer_state.TableState) error {
	currDatabaseOperations := s.operationsForTable(tableState)
	if err := currDatabaseOperations.Publish(ctx); err != nil {
		return xerrors.Errorf("publishAndCleanupTableOnce: %w", err)
	}
	if err := currDatabaseOperations.DropPreviousTables(ctx); err != nil {
		return xerrors.Errorf("publishAndCleanupTableOnce: %w", err)
	}
	return tableState.CompletePublication(ctx)
}

// publishAndCleanupTable retries publication and removal of the previous tables
// until the database-specific publication barrier passes or the context is canceled.
func (s *Sink) publishAndCleanupTable(ctx context.Context, tableState *transfer_state.TableState) error {
	for {
		err := s.publishAndCleanupTableOnce(ctx, tableState)
		if err == nil {
			return nil
		}
		if tableState.StateWritesBlocked() {
			return xerrors.Errorf("publishAndCleanupTable: %w", err)
		}
		s.logger.Warn("Finishing ClickHouse snapshot publication; retrying", log.String("table", tableState.DestinationTableName()), log.Error(err))
		if err := ch_eo_common.WaitOrCancel(ctx, 5*time.Second); err != nil {
			return xerrors.Errorf("publishAndCleanupTable: %w", err)
		}
	}
}

// Cleanup runs after all snapshot workers have stopped.
// It discovers all generations; it never removes CDC or other providers' state.
func (s *Sink) Cleanup(ctx context.Context) error {
	return s.cleanupSnapshotStateAndResources(ctx, false)
}

// ResetPreviousOperations removes old operations' temporary resources, retaining final
// tables as they are. Previous workers must have stopped before this is called.
// Recreating a sink in the current operation does not reset its state.
func (s *Sink) ResetPreviousOperations(ctx context.Context) error {
	if s.publicationStore.OperationID() == "" {
		return xerrors.New("reset previous snapshot requires an operation ID")
	}
	return s.cleanupSnapshotStateAndResources(ctx, true)
}

func (s *Sink) cleanupSnapshotStateAndResources(ctx context.Context, previousOperationsOnly bool) error {
	if !previousOperationsOnly {
		s.stopStagingDDLKeepalives()
	}
	tableStateKeysByOperation, err := s.publicationStore.ListTableStateKeysByOperation(ctx)
	if err != nil {
		return xerrors.Errorf("read snapshot operations: %w", err)
	}
	if len(tableStateKeysByOperation) != 0 {
		if err := s.initDatabaseOperations(ctx); err != nil {
			return xerrors.Errorf("cleanup databaseOperations: %w", err)
		}
	}

	operationIDs := make([]string, 0, len(tableStateKeysByOperation))
	for operationID := range tableStateKeysByOperation {
		operationIDs = append(operationIDs, operationID)
	}
	sort.Strings(operationIDs)
	for _, operationID := range operationIDs {
		if previousOperationsOnly && operationID == s.publicationStore.OperationID() {
			continue
		}
		stagingDDLSessions.stop(s.stagingDDLOperationKey(operationID))
		// Cleanup reads durable state afresh, including a write whose reply was lost.
		operationStore := s.publicationStore.ForOperation(operationID)
		if err := s.cleanupOperationResources(ctx, operationStore, tableStateKeysByOperation[operationID], previousOperationsOnly); err != nil {
			return xerrors.Errorf("cleanupSnapshotStateAndResources: %w", err)
		}
	}
	return nil
}

func (s *Sink) cleanupOperationResources(ctx context.Context, operationStore *transfer_state.State, tableStateKeys []string, discardPublication bool) error {
	for _, tableStateKey := range tableStateKeys {
		tableState, err := operationStore.LoadTable(ctx, tableStateKey)
		if err != nil {
			return xerrors.Errorf("cleanupOperationResources: %w", err)
		}
		if tableState == nil {
			return xerrors.New("snapshot table disappeared during cleanup")
		}
		if err := s.closeTableStaging(ctx, tableState); err != nil {
			return xerrors.Errorf("cleanupOperationResources: %w", err)
		}
		if err := s.operationsForTable(tableState).WaitForPendingCommandsAndClearRecords(ctx); err != nil {
			return xerrors.Errorf("cleanup pending commands: %w", err)
		}
		if discardPublication && !tableState.DiscardOnCleanup() {
			if err := tableState.DiscardPublication(ctx); err != nil {
				return xerrors.Errorf("cleanupOperationResources: %w", err)
			}
		}
		if tableState.DiscardOnCleanup() {
			if err := s.operationsForTable(tableState).PreparePreviousPublicationForCleanup(ctx); err != nil {
				return xerrors.Errorf("cleanupOperationResources: %w", err)
			}
		} else if tableState.PublicationDecided() && !tableState.PublicationComplete() {
			if err := s.publishAndCleanupTable(ctx, tableState); err != nil {
				return xerrors.Errorf("cleanupOperationResources: %w", err)
			}
		}
		if tableState.PublicationComplete() && !tableState.DiscardOnCleanup() {
			// A returned replica may still need its candidate to publish the table.
			if err := s.operationsForTable(tableState).EnsurePublicationBeforeCleanup(ctx); err != nil {
				return xerrors.Errorf("cleanupOperationResources: %w", err)
			}
		}
		if err := s.operationsForTable(tableState).DropTemporaryTables(ctx); err != nil {
			return xerrors.Errorf("cleanupOperationResources: %w", err)
		}
		if err := operationStore.RemoveTableState(ctx, tableStateKey); err != nil {
			return xerrors.Errorf("cleanupOperationResources: %w", err)
		}
	}
	return nil
}

func (s *Sink) stagingDDLOperationKey(operationID string) string {
	return stagingDDLSessionOperationKey(s.transferID, operationID)
}

func (s *Sink) startStagingDDLKeepalive(hostName, sessionID string) error {
	return stagingDDLSessions.start(s.stagingDDLOperationKey(s.publicationStore.OperationID()), sessionID, func() (*stagingDDLSessionKeepalive, error) {
		node, err := s.tablesClient.NodeByHostName(hostName)
		if err != nil {
			return nil, xerrors.Errorf("find staging DDL host: %w", err)
		}
		params, err := s.dstConfig.ToSinkParams(new(model.Transfer))
		if err != nil {
			return nil, xerrors.Errorf("prepare staging DDL client: %w", err)
		}
		client, err := httpclient.NewHTTPClientImpl(params)
		if err != nil {
			return nil, xerrors.Errorf("create staging DDL client: %w", err)
		}
		host := *node.Host
		return newStagingDDLSessionKeepalive(s.logger, client, &host, client.CloseIdleConnections, sessionID, 20*time.Second), nil
	})
}

func (s *Sink) stopStagingDDLKeepalives() {
	stagingDDLSessions.stop(s.stagingDDLOperationKey(s.publicationStore.OperationID()))
}

// initDatabaseOperations runs before any snapshot table is created.
func (s *Sink) initDatabaseOperations(ctx context.Context) error {
	if s.databaseOperations != nil {
		return nil
	}
	databaseOperations, err := database_operations.New(ctx, s.logger, s.transferID, s.dstConfig.Database, s.dstConfig.Cleanup, s.tablesClient)
	if err != nil {
		return xerrors.Errorf("initDatabaseOperations: %w", err)
	}
	s.databaseOperations = databaseOperations
	return nil
}

// operationsForTable is the persistence boundary. SQL algorithms receive detached
// inputs and report command checkpoints; only the sink writes TransferState.
func (s *Sink) operationsForTable(tableState *transfer_state.TableState) database_operations.DatabaseOperations {
	return s.databaseOperations.ForTable(tableState.Snapshot(), func(ctx context.Context, hostName string, ownedTable *ch_eo_common.OwnedTable, command *ch_eo_common.Command) error {
		var err error
		if ownedTable == nil {
			err = tableState.RecordPublicationCommand(ctx, hostName, command)
		} else if command == nil {
			err = tableState.ClearPendingTableCommand(ctx, ownedTable.ExpectedTableUUID)
		} else {
			err = tableState.RecordPendingTableCommand(ctx, ownedTable, command)
		}
		if err != nil {
			return xerrors.Errorf("save SQL command checkpoint: %w", err)
		}
		return nil
	})
}

func (s *Sink) closeTableStaging(ctx context.Context, tableState *transfer_state.TableState) error {
	if err := s.operationsForTable(tableState).CloseStaging(ctx); err != nil {
		return xerrors.Errorf("close staging DDL: %w", err)
	}
	if tableState.StagingClosed() {
		return nil
	}
	return tableState.CloseStaging(ctx)
}

func (s *Sink) buildCandidateTable(ctx context.Context, tableState *transfer_state.TableState, readyParts []ch_eo_common.ReadyPart) error {
	candidates, err := s.operationsForTable(tableState).BuildCandidates(ctx, readyParts)
	if err != nil {
		return xerrors.Errorf("build candidate table: %w", err)
	}
	return tableState.RecordCandidates(ctx, candidates)
}

// snapshotDDL uses the immutable Metrika definition supplied during table discovery.
func (s *Sink) snapshotDDL(item abstract.ChangeItem) (string, error) {
	columns := item.TableSchema.Columns()
	if len(columns) != 0 {
		snapshotDDLProperty, hasSnapshotDDL := columns[0].Properties[abstract.PropertyKey(ddlbuilder.SnapshotDDLProperty)]
		if hasSnapshotDDL {
			sourceTableDDL, isString := snapshotDDLProperty.(string)
			if !isString || sourceTableDDL == "" {
				return "", xerrors.New("invalid Metrika snapshot DDL in source schema")
			}
			return sourceTableDDL, nil
		}
	}
	sourceTableDDL, err := s.destinationDDLFunc(item)
	if err != nil {
		return "", xerrors.Errorf("build snapshot DDL: %w", err)
	}
	return sourceTableDDL, nil
}

// checkMetrikaDDL does not execute source DDL against the target. Main already
// registered its definition; publication and row-count checks remain unchanged.
func (s *Sink) checkMetrikaDDL(ctx context.Context, item abstract.ChangeItem) error {
	if len(item.ColumnValues) != 1 {
		return xerrors.New("Metrika DDL event must contain one builder")
	}
	metrikaDDLBuilder, isBuilder := item.ColumnValues[0].(ddlbuilder.ClickHouseBuilder)
	if !isBuilder {
		return xerrors.New("Metrika DDL event contains an invalid builder")
	}
	sourceTableDDL, err := metrikaDDLBuilder.BuildClickHouseDDL("", "", "", false)
	if err != nil {
		return xerrors.Errorf("build Metrika DDL: %w", err)
	}
	tableState, err := s.publicationStore.LoadTable(ctx, s.publicationStore.TableStateKey(item.TableID()))
	if err != nil {
		return xerrors.Errorf("read Metrika snapshot definition: %w", err)
	}
	if tableState == nil || tableState.StagingClosed() || tableState.PublicationDecided() {
		return xerrors.New("Metrika snapshot table is not open")
	}
	tableReplicas := tableState.Replicas()
	if len(tableReplicas) == 0 {
		return xerrors.New("Metrika snapshot has no initialized replicas")
	}
	if s.dstConfig.Cleanup == model.Drop || tableReplicas[0].PreviousTableUUID == "" {
		if sourceTableDDL != tableState.DDLTemplate() {
			return xerrors.New("Metrika DDL event differs from the initialized source schema")
		}
	}
	return nil
}
