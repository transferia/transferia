package transfer_state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
)

func TestTableOwnsAllMutableState(test *testing.T) {
	ctx := context.Background()
	storage := newSerialState()
	store := NewState(storage, "transfer", "operation")
	table := store.NewTable(abstract.TableID{Name: "t"}, "t", "", false)
	table.AddReplica("host", 0, "old")
	sessions := map[string]string{"host": "session"}
	require.NoError(test, table.RegisterStaging(ctx, "__prefix_", sessions))
	sessions["host"] = "changed input"
	table.StagingSessions()["host"] = "changed output"
	resource := ch_eo_common.NewOwnedTable("host", "candidate", "uuid")
	var candidates []ch_eo_common.NodeState
	candidates = append(candidates, ch_eo_common.NewCandidate("host", 0, *resource, 42))
	resource.TableName = "changed input"
	require.NoError(test, table.RecordCandidates(ctx, candidates))
	candidates[0].CandidateTable.TableName = "changed committed result"
	replicas := table.Replicas()
	replicas[0].HostName = "changed host"
	replicas[0].CandidateTable.PendingCommand = ch_eo_common.NewCommand("changed", "changed")
	candidateView := table.Replicas()[0].CandidateTable
	candidateView.TableName = "changed returned value"
	require.Equal(test, "changed returned value", candidateView.TableName)
	command := ch_eo_common.NewCommand("command-session", "CREATE TABLE candidate")
	pending := ch_eo_common.NewOwnedTable("host", "temporary", "pending-uuid")
	require.NoError(test, table.RecordPendingTableCommand(ctx, pending, command))
	require.Nil(test, pending.PendingCommand)
	command.SQL = "changed input"
	table.TableWithPendingCommand("pending-uuid").PendingCommand.SQL = "changed output"
	publicationCommand := ch_eo_common.NewCommand("publish-session", "EXCHANGE TABLES t AND candidate")
	require.NoError(test, table.RecordPublicationCommand(ctx, "host", publicationCommand))
	publicationCommand.SQL = "changed input"
	snapshot := table.Snapshot()
	snapshot.DestinationTableName = "changed destination"
	snapshot.StagingDDLSessionIDs["host"] = "changed session"
	snapshot.NodeStates[0].CandidateTable.TableName = "changed candidate"
	snapshot.NodeStates[0].PendingPublicationCommand.SQL = "changed publication"
	snapshot.PendingTableCommands["pending-uuid"].PendingCommand.SQL = "changed pending command"
	delete(snapshot.PendingTableCommands, "pending-uuid")
	require.Equal(test, "t", table.DestinationTableName())
	table.Replicas()[0].PendingPublicationCommand.SQL = "changed output"
	for _, actual := range []*TableState{table, func() *TableState {
		loaded, err := store.LoadTable(ctx, table.StateKey())
		require.NoError(test, err)
		return loaded
	}()} {
		require.Equal(test, "session", actual.StagingSessions()["host"])
		require.Equal(test, "candidate", actual.Replicas()[0].CandidateTable.TableName)
		require.EqualValues(test, 42, actual.Replicas()[0].ExpectedRowCount)
		require.Equal(test, "CREATE TABLE candidate", actual.TableWithPendingCommand("pending-uuid").PendingCommand.SQL)
		require.Equal(test, "EXCHANGE TABLES t AND candidate", actual.Replicas()[0].PendingPublicationCommand.SQL)
	}
}

func TestPublicationTransitionsPersistWithoutRebinding(test *testing.T) {
	ctx := context.Background()
	store := NewState(newSerialState(), "transfer", "operation")
	first := store.NewTable(abstract.TableID{Name: "first"}, "first", "", false)
	second := store.NewTable(abstract.TableID{Name: "second"}, "second", "", false)
	require.NoError(test, first.RegisterStaging(ctx, "__first_", map[string]string{"host": "session"}))
	require.NoError(test, second.RegisterStaging(ctx, "__second_", nil))
	require.NoError(test, first.CloseStaging(ctx))
	require.NoError(test, first.DecidePublication(ctx))
	require.NoError(test, second.DiscardPublication(ctx))
	require.NoError(test, first.CompletePublication(ctx))
	require.NoError(test, first.CloseStaging(ctx))
	require.NoError(test, first.DecidePublication(ctx))
	restored, err := store.LoadTable(ctx, first.StateKey())
	require.NoError(test, err)
	require.True(test, restored.StagingClosed())
	require.Nil(test, restored.StagingSessions())
	require.True(test, restored.PublicationDecided())
	require.True(test, restored.PublicationComplete())
	require.False(test, restored.DiscardOnCleanup())
	restored, err = store.LoadTable(ctx, second.StateKey())
	require.NoError(test, err)
	require.False(test, restored.StagingClosed())
	require.NotNil(test, restored.StagingSessions())
	require.False(test, restored.PublicationDecided())
	require.True(test, restored.DiscardOnCleanup())
}

func TestDiscardRetainsRecoveryMetadata(test *testing.T) {
	for _, completed := range []bool{false, true} {
		store := NewState(newSerialState(), "transfer", "operation")
		table := store.NewTable(abstract.TableID{Name: "table"}, "table", "", true)
		table.AddReplica("host", 0, "previous-uuid")
		var candidates []ch_eo_common.NodeState
		candidates = append(candidates, ch_eo_common.NewCandidate("host", 0, *ch_eo_common.NewOwnedTable("host", "candidate", "candidate-uuid"), 42))
		ctx := context.Background()
		require.NoError(test, table.RecordCandidates(ctx, candidates))
		require.NoError(test, table.DecidePublication(ctx))
		if completed {
			require.NoError(test, table.CompletePublication(ctx))
		}
		require.NoError(test, table.DiscardPublication(ctx))
		restored, err := store.LoadTable(ctx, table.StateKey())
		require.NoError(test, err)
		require.True(test, restored.DiscardOnCleanup())
		require.False(test, restored.PublicationDecided())
		require.False(test, restored.PublicationComplete())
		require.Equal(test, "previous-uuid", restored.Replicas()[0].PreviousTableUUID)
		require.Equal(test, "candidate-uuid", restored.Replicas()[0].CandidateTable.ExpectedTableUUID)
		require.EqualValues(test, 42, restored.Replicas()[0].ExpectedRowCount)
	}
}

func TestRecordCandidatesReplacesWholeGeneration(test *testing.T) {
	ctx := context.Background()
	storage := newSerialState()
	store := NewState(storage, "transfer", "operation")
	sourceTableID := new(abstract.TableID)
	sourceTableID.Name = "table"
	table := store.NewTable(*sourceTableID, "table", "", true)
	table.AddReplica("host-a", 0, "previous-a")
	table.AddReplica("host-b", 0, "previous-b")
	firstCandidates := []ch_eo_common.NodeState{
		ch_eo_common.NewCandidate("host-a", 0, *ch_eo_common.NewOwnedTable("host-a", "candidate-a1", "uuid-a1"), 11),
		ch_eo_common.NewCandidate("host-b", 0, *ch_eo_common.NewOwnedTable("host-b", "candidate-b1", "uuid-b1"), 11),
	}
	require.NoError(test, table.RecordCandidates(ctx, firstCandidates))

	// Recover before the publication decision. Only host A is available for
	// the new build, so B's old candidate must not survive its registration.
	recovered, err := NewState(storage, "transfer", "operation").LoadTable(ctx, table.StateKey())
	require.NoError(test, err)
	newCandidate := ch_eo_common.NewOwnedTable("host-a", "candidate-a2", "uuid-a2")
	require.NoError(test, recovered.RecordCandidates(ctx, []ch_eo_common.NodeState{
		ch_eo_common.NewCandidate("host-a", 0, *newCandidate, 17),
	}))
	restored, err := NewState(storage, "transfer", "operation").LoadTable(ctx, table.StateKey())
	require.NoError(test, err)
	expectedA := ch_eo_common.NewNodeState("host-a", 0, "previous-a")
	expectedA.CandidateTable = *newCandidate
	expectedA.ExpectedRowCount = 17
	expectedB := ch_eo_common.NewNodeState("host-b", 0, "previous-b")
	for _, actual := range []*TableState{recovered, restored} {
		require.Equal(test, []ch_eo_common.NodeState{expectedA, expectedB}, actual.Replicas())
	}
}

func TestRecordCandidatesRejectsDecidedPublication(test *testing.T) {
	for _, completed := range []bool{false, true} {
		name := "decided"
		if completed {
			name = "complete"
		}
		test.Run(name, func(test *testing.T) {
			ctx := context.Background()
			storage := newSerialState()
			store := NewState(storage, "transfer", "operation")
			sourceTableID := new(abstract.TableID)
			sourceTableID.Name = "table"
			table := store.NewTable(*sourceTableID, "table", "", true)
			table.AddReplica("host", 0, "previous")
			require.NoError(test, table.RecordCandidates(ctx, []ch_eo_common.NodeState{
				ch_eo_common.NewCandidate("host", 0, *ch_eo_common.NewOwnedTable("host", "verified", "verified-uuid"), 11),
			}))
			require.NoError(test, table.DecidePublication(ctx))
			if completed {
				require.NoError(test, table.CompletePublication(ctx))
			}
			before := table.Snapshot()

			err := table.RecordCandidates(ctx, []ch_eo_common.NodeState{
				ch_eo_common.NewCandidate("host", 0, *ch_eo_common.NewOwnedTable("host", "replacement", "replacement-uuid"), 17),
			})

			require.ErrorContains(test, err, "publication is decided")
			require.Equal(test, before, table.Snapshot())
			restored, err := NewState(storage, "transfer", "operation").LoadTable(ctx, table.StateKey())
			require.NoError(test, err)
			require.Equal(test, before, restored.Snapshot())
		})
	}
}
