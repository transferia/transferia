package snapshot

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/database_operations"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/transfer_state"
)

type publicationRecoveryOperations struct {
	database_operations.DatabaseOperations
	test             *testing.T
	candidate        ch_eo_common.NodeState
	events           *[]string
	publicationStore *transfer_state.State
	tableStateKey    string
}

var _ database_operations.DatabaseOperations = (*publicationRecoveryOperations)(nil)

func (s *publicationRecoveryOperations) ForTable(table *ch_eo_common.SnapshotTable, _ database_operations.CommandCheckpoint) database_operations.DatabaseOperations {
	require.True(s.test, table.PublicationDecided())
	require.Equal(s.test, []ch_eo_common.NodeState{s.candidate}, table.NodeStates)
	bound := *s
	return &bound
}

func (s *publicationRecoveryOperations) BuildCandidates(context.Context, []ch_eo_common.ReadyPart) ([]ch_eo_common.NodeState, error) {
	return nil, xerrors.New("recovery must use the recorded candidate")
}

func (s *publicationRecoveryOperations) Publish(context.Context) error {
	*s.events = append(*s.events, "publish")
	return nil
}

func (s *publicationRecoveryOperations) DropPreviousTables(context.Context) error {
	*s.events = append(*s.events, "drop_previous")
	return nil
}

func (s *publicationRecoveryOperations) CloseStaging(context.Context) error {
	if len(*s.events) == 0 || (*s.events)[0] != "publish" {
		return xerrors.New("recovery must publish before closing staging")
	}
	*s.events = append(*s.events, "close_staging")
	return nil
}

func (s *publicationRecoveryOperations) WaitForPendingCommandsAndClearRecords(context.Context) error {
	*s.events = append(*s.events, "wait_pending")
	return nil
}

func (s *publicationRecoveryOperations) EnsurePublicationBeforeCleanup(context.Context) error {
	*s.events = append(*s.events, "ensure_publication")
	return nil
}

func (s *publicationRecoveryOperations) DropTemporaryTables(ctx context.Context) error {
	require.Equal(s.test, "ensure_publication", (*s.events)[len(*s.events)-1])
	table, err := s.publicationStore.LoadTable(ctx, s.tableStateKey)
	require.NoError(s.test, err)
	require.NotNil(s.test, table, "the recovery journal must survive until temporary tables are removed")
	require.True(s.test, table.PublicationComplete())
	*s.events = append(*s.events, "drop_temporary")
	return nil
}

func TestCommitResumesRecordedPublication(test *testing.T) {
	for _, publicationStatus := range []string{ch_eo_common.PublicationDecided, ch_eo_common.PublicationComplete} {
		test.Run(publicationStatus, func(test *testing.T) {
			ctx := context.Background()
			storage := newSerialState()
			store := transfer_state.NewState(storage, "transfer", "operation")
			sourceTableID := abstract.TableID{Namespace: "source", Name: "events"}
			table := store.NewTable(sourceTableID, "events", "", false)
			table.AddReplica("host", 0, "")
			require.NoError(test, table.RegisterStaging(ctx, "__recovery_", nil))
			candidateTable := ch_eo_common.NewTemporaryTable(table.TemporaryTablePrefix(), "host", ch_eo_common.RoleCandidate)
			candidate := ch_eo_common.NewCandidate("host", 0, *candidateTable, 10)
			require.NoError(test, table.RecordCandidates(ctx, []ch_eo_common.NodeState{candidate}))
			require.NoError(test, table.CloseStaging(ctx))
			require.NoError(test, table.DecidePublication(ctx))
			if publicationStatus == ch_eo_common.PublicationComplete {
				require.NoError(test, table.CompletePublication(ctx))
			}

			// A restarted sink has only the serialized publication journal.
			recoveredStore := transfer_state.NewState(storage, "transfer", "operation")
			events := make([]string, 0)
			operations := &publicationRecoveryOperations{
				DatabaseOperations: nil,
				test:               test,
				candidate:          candidate,
				events:             &events,
				publicationStore:   recoveredStore,
				tableStateKey:      recoveredStore.TableStateKey(sourceTableID),
			}
			sink := &Sink{
				logger:                       logger.Log,
				transferID:                   "transfer",
				dstConfig:                    nil,
				tablesClient:                 nil,
				mu:                           sync.Mutex{},
				publicationStore:             recoveredStore,
				defaultReplicatedTableEngine: false,
				sourceTableByDestinationName: nil,
				databaseOperations:           operations,
				destinationTableNameFunc:     nil,
				destinationDDLFunc:           nil,
				uploader:                     nil,
				closeHTTPClient:              nil,
				closed:                       false,
				poison:                       nil,
			}

			require.NoError(test, sink.Commit(ctx))
			require.Equal(test, []string{"publish", "drop_previous", "close_staging", "wait_pending", "ensure_publication", "drop_temporary"}, events)
			recoveredTable, err := recoveredStore.LoadTable(ctx, operations.tableStateKey)
			require.NoError(test, err)
			require.Nil(test, recoveredTable)
		})
	}
}
