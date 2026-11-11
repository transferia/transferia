package transfer_state

import (
	"context"
	"maps"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

// TableState owns one source table's durable publication record. It never exposes a
// mutable alias of that record. All durable transitions serialize mutation and save.
type TableState struct {
	mutex  sync.Mutex
	record ch_eo_common.SnapshotTable
	store  *State
}

func newTableState(store *State, record ch_eo_common.SnapshotTable) *TableState {
	return &TableState{mutex: sync.Mutex{}, record: record, store: store}
}

func (t *TableState) StateKey() string { return t.store.TableStateKey(t.record.SourceTableID) }

func (t *TableState) SourceTableID() abstract.TableID { return t.record.SourceTableID }

func (t *TableState) DestinationTableName() string { return t.record.DestinationTableName }

func (t *TableState) DDLTemplate() string { return t.record.DDLTemplate }

func (t *TableState) UseReplicatedTableEngine() bool { return t.record.UseReplicatedTableEngine }

func (t *TableState) TemporaryTablePrefix() string { return t.record.TemporaryTablePrefix }

// UseDefinition and AddReplica configure a new table before RegisterStaging.
func (t *TableState) UseDefinition(ddlTemplate string, replicated bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.record.DDLTemplate, t.record.UseReplicatedTableEngine = ddlTemplate, replicated
}

func (t *TableState) AddReplica(hostName string, shardIndex int, previousTableUUID string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.record.NodeStates = append(t.record.NodeStates, ch_eo_common.NewNodeState(hostName, shardIndex, previousTableUUID))
}

func (t *TableState) Replicas() []ch_eo_common.NodeState {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	result := make([]ch_eo_common.NodeState, 0, len(t.record.NodeStates))
	for _, node := range t.record.NodeStates {
		node.CandidateTable = *ch_eo_common.CopyOwnedTable(&node.CandidateTable)
		node.PendingPublicationCommand = ch_eo_common.CopyCommand(node.PendingPublicationCommand)
		result = append(result, node)
	}
	return result
}

func (t *TableState) StagingSessions() map[string]string {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return maps.Clone(t.record.StagingDDLSessionIDs)
}

func (t *TableState) StagingClosed() bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.record.StagingDDLSessionIDs == nil
}

func (t *TableState) PublicationDecided() bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.record.PublicationDecided()
}

func (t *TableState) PublicationComplete() bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.record.PublicationStatus == ch_eo_common.PublicationComplete
}

func (t *TableState) DiscardOnCleanup() bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.record.PublicationStatus == ch_eo_common.PublicationDiscarded
}

func (t *TableState) StateWritesBlocked() bool { return t.store.writesBlocked() }

func (t *TableState) RegisterStaging(ctx context.Context, prefix string, sessionIDs map[string]string) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.record.TemporaryTablePrefix = prefix
	t.record.StagingDDLSessionIDs = make(map[string]string, len(sessionIDs))
	maps.Copy(t.record.StagingDDLSessionIDs, sessionIDs)
	return t.store.saveRecord(ctx, &t.record)
}

func (t *TableState) CloseStaging(ctx context.Context) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.record.StagingDDLSessionIDs = nil
	return t.store.saveRecord(ctx, &t.record)
}

func (t *TableState) RecordCandidates(ctx context.Context, candidates []ch_eo_common.NodeState) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.record.PublicationDecided() {
		return xerrors.New("cannot replace candidates after publication is decided")
	}
	// A rebuild replaces the whole generation. A skipped replica must not keep
	// a candidate from the previous build, which used a different Keeper path.
	for nodeIndex := range t.record.NodeStates {
		node := &t.record.NodeStates[nodeIndex]
		node.CandidateTable = *ch_eo_common.NewOwnedTable("", "", "")
		node.ExpectedRowCount = 0
		for _, candidate := range candidates {
			if node.HostName == candidate.HostName && node.ShardIndex == candidate.ShardIndex {
				node.CandidateTable = *ch_eo_common.CopyOwnedTable(&candidate.CandidateTable)
				node.ExpectedRowCount = candidate.ExpectedRowCount
				break
			}
		}
	}
	return t.store.saveRecord(ctx, &t.record)
}

func (t *TableState) DecidePublication(ctx context.Context) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.record.PublicationStatus == ch_eo_common.PublicationNotDecided {
		t.record.PublicationStatus = ch_eo_common.PublicationDecided
	}
	return t.store.saveRecord(ctx, &t.record)
}

func (t *TableState) CompletePublication(ctx context.Context) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.record.PublicationStatus = ch_eo_common.PublicationComplete
	return t.store.saveRecord(ctx, &t.record)
}

func (t *TableState) DiscardPublication(ctx context.Context) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.record.PublicationStatus = ch_eo_common.PublicationDiscarded
	return t.store.saveRecord(ctx, &t.record)
}

func (t *TableState) RecordPublicationCommand(ctx context.Context, hostName string, command *ch_eo_common.Command) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for nodeIndex := range t.record.NodeStates {
		node := &t.record.NodeStates[nodeIndex]
		if node.HostName == hostName {
			node.PendingPublicationCommand = ch_eo_common.CopyCommand(command)
			return t.store.saveRecord(ctx, &t.record)
		}
	}
	return xerrors.Errorf("publication host %s is absent from table state", hostName)
}

func (t *TableState) ResolvePublicationCommand(ctx context.Context, hostName string) error {
	return t.RecordPublicationCommand(ctx, hostName, nil)
}

// RecordPendingTableCommand persists admission before the caller submits SQL.
func (t *TableState) RecordPendingTableCommand(ctx context.Context, ownedTable *ch_eo_common.OwnedTable, command *ch_eo_common.Command) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.record.PendingTableCommands[ownedTable.ExpectedTableUUID] != nil {
		return xerrors.New("previous table command is unresolved")
	}
	pendingTable := ch_eo_common.CopyOwnedTable(ownedTable)
	pendingTable.PendingCommand = ch_eo_common.CopyCommand(command)
	if t.record.PendingTableCommands == nil {
		t.record.PendingTableCommands = make(map[string]*ch_eo_common.OwnedTable)
	}
	t.record.PendingTableCommands[ownedTable.ExpectedTableUUID] = pendingTable
	return t.store.saveRecord(ctx, &t.record)
}

// ClearPendingTableCommand follows confirmation that the SQL/DDL has stopped.
func (t *TableState) ClearPendingTableCommand(ctx context.Context, tableUUID string) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	delete(t.record.PendingTableCommands, tableUUID)
	return t.store.saveRecord(ctx, &t.record)
}

func (t *TableState) PendingTableUUIDs() []string {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	result := make([]string, 0, len(t.record.PendingTableCommands))
	for tableUUID := range t.record.PendingTableCommands {
		result = append(result, tableUUID)
	}
	return result
}

func (t *TableState) TableWithPendingCommand(tableUUID string) *ch_eo_common.OwnedTable {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return ch_eo_common.CopyOwnedTable(t.record.PendingTableCommands[tableUUID])
}

// Snapshot returns independent data for ClickHouse operations. Changes to this
// value never modify the durable record without an explicit TableState method call.
func (t *TableState) Snapshot() *ch_eo_common.SnapshotTable {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	result := t.record
	result.StagingDDLSessionIDs = maps.Clone(t.record.StagingDDLSessionIDs)
	result.PendingTableCommands = make(map[string]*ch_eo_common.OwnedTable, len(t.record.PendingTableCommands))
	for tableUUID, ownedTable := range t.record.PendingTableCommands {
		result.PendingTableCommands[tableUUID] = ch_eo_common.CopyOwnedTable(ownedTable)
	}
	result.NodeStates = make([]ch_eo_common.NodeState, len(t.record.NodeStates))
	for nodeIndex, node := range t.record.NodeStates {
		node.CandidateTable = *ch_eo_common.CopyOwnedTable(&node.CandidateTable)
		node.PendingPublicationCommand = ch_eo_common.CopyCommand(node.PendingPublicationCommand)
		result.NodeStates[nodeIndex] = node
	}
	return &result
}
