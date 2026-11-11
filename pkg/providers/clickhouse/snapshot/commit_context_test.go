package snapshot

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	ch_eo_common "github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/common"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/database_operations"
	"github.com/transferia/transferia/pkg/providers/clickhouse/snapshot/transfer_state"
	"github.com/transferia/transferia/pkg/worker/tasks"
	mockstorage "github.com/transferia/transferia/tests/helpers/mock_storage"
)

type commitPublicationState struct {
	destinationUUID string
	exchanges       int
	publishStarted  chan<- struct{}
	publishRelease  <-chan struct{}
}

type commitPublicationOperations struct {
	database_operations.DatabaseOperations
	state      *commitPublicationState
	table      *ch_eo_common.SnapshotTable
	checkpoint database_operations.CommandCheckpoint
}

var _ database_operations.DatabaseOperations = (*commitPublicationOperations)(nil)

func (s *commitPublicationOperations) ForTable(table *ch_eo_common.SnapshotTable, checkpoint database_operations.CommandCheckpoint) database_operations.DatabaseOperations {
	bound := *s
	bound.table = table
	bound.checkpoint = checkpoint
	return &bound
}

func (s *commitPublicationOperations) Publish(ctx context.Context) error {
	node := s.table.NodeStates[0]
	if s.state.destinationUUID != node.CandidateTable.ExpectedTableUUID {
		command := ch_eo_common.NewCommand("publication-session", "EXCHANGE TABLES events AND candidate")
		if err := s.checkpoint(ctx, node.HostName, nil, command); err != nil {
			return err
		}
		s.state.destinationUUID = node.CandidateTable.ExpectedTableUUID
		s.state.exchanges++
	}
	if s.state.publishStarted != nil {
		select {
		case s.state.publishStarted <- struct{}{}:
		default:
		}
	}
	if s.state.publishRelease != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.state.publishRelease:
		}
	}
	return s.checkpoint(ctx, node.HostName, nil, nil)
}

func (s *commitPublicationOperations) DropPreviousTables(ctx context.Context) error {
	node := s.table.NodeStates[0]
	previousTable := node.CandidateTable
	previousTable.ExpectedTableUUID = node.PreviousTableUUID
	if err := s.checkpoint(ctx, node.HostName, &previousTable, nil); err != nil {
		return err
	}
	return nil
}

func (s *commitPublicationOperations) CloseStaging(context.Context) error { return nil }

func (s *commitPublicationOperations) WaitForPendingCommandsAndClearRecords(context.Context) error {
	return nil
}

func (s *commitPublicationOperations) EnsurePublicationBeforeCleanup(ctx context.Context) error {
	return s.Publish(ctx)
}

func (s *commitPublicationOperations) DropTemporaryTables(context.Context) error { return nil }

// The source upload has already produced a durable publication decision. Loader
// control sinks only need to forward its final commit to this recovered sink.
type loaderCommitSink struct{ *Sink }

func (s *loaderCommitSink) Push([]abstract.ChangeItem) error { return nil }
func (s *loaderCommitSink) Close() error                     { return nil }

func newCommitRecoverySink(store *transfer_state.State, operations database_operations.DatabaseOperations) *Sink {
	uploadContext, cancelUpload := context.WithCancel(context.Background())
	return &Sink{
		logger:                       logger.Log,
		transferID:                   "transfer",
		dstConfig:                    &clickhouse_model.ChDestination{Database: "test"},
		tablesClient:                 nil,
		mu:                           sync.Mutex{},
		publicationStore:             store,
		defaultReplicatedTableEngine: false,
		sourceTableByDestinationName: nil,
		databaseOperations:           operations,
		destinationTableNameFunc:     nil,
		destinationDDLFunc:           nil,
		uploader:                     nil,
		closeHTTPClient:              nil,
		uploadContext:                uploadContext,
		cancelUpload:                 cancelUpload,
		closed:                       false,
		poison:                       nil,
	}
}

func newRecordedCommitSink(test *testing.T, storage coordinator.TransferState, operationID string) (*Sink, *commitPublicationState, string) {
	test.Helper()
	store := transfer_state.NewState(storage, "transfer", operationID)
	tableID := abstract.TableID{Namespace: "source", Name: "events"}
	state, stateKey := recordCommitTable(test, store, tableID, "events")
	operations := &commitPublicationOperations{DatabaseOperations: nil, state: state, table: nil, checkpoint: nil}
	recoveredStore := transfer_state.NewState(storage, "transfer", operationID)
	return newCommitRecoverySink(recoveredStore, operations), state, stateKey
}

func recordCommitTable(test *testing.T, store *transfer_state.State, tableID abstract.TableID, destinationName string) (*commitPublicationState, string) {
	test.Helper()
	ctx := context.Background()
	table := store.NewTable(tableID, destinationName, "", false)
	previousUUID := uuid.NewString()
	table.AddReplica("host", 0, previousUUID)
	require.NoError(test, table.RegisterStaging(ctx, "__"+destinationName+"_", nil))
	candidate := ch_eo_common.NewTemporaryTable(table.TemporaryTablePrefix(), "host", ch_eo_common.RoleCandidate)
	require.NoError(test, table.RecordCandidates(ctx, []ch_eo_common.NodeState{ch_eo_common.NewCandidate("host", 0, *candidate, 10)}))
	require.NoError(test, table.CloseStaging(ctx))
	require.NoError(test, table.DecidePublication(ctx))
	state := &commitPublicationState{
		destinationUUID: previousUUID,
		exchanges:       0,
		publishStarted:  nil,
		publishRelease:  nil,
	}
	return state, store.TableStateKey(tableID)
}

func TestLoaderCancelsSnapshotCommitAndKeepsPublicationJournal(test *testing.T) {
	sink, publication, stateKey := newRecordedCommitSink(test, newSerialState(), "operation")
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	publication.publishStarted = started
	publication.publishRelease = release
	var releaseOnce sync.Once
	releasePublish := func() { releaseOnce.Do(func() { close(release) }) }
	storage := mockstorage.NewMockStorage()
	storage.TableSchemaF = func(context.Context, abstract.TableID) (*abstract.TableSchema, error) { return snapshotTestSchema, nil }
	source := new(model.MockSource)
	source.StorageFactory = func() abstract.Storage { return storage }
	destination := &model.MockDestination{SinkerFactory: func() abstract.Sinker { return &loaderCommitSink{Sink: sink} }, Cleanup: model.DisabledCleanup}
	runtime := new(abstract.LocalRuntime)
	runtime.ShardingUpload.JobCount = 1
	runtime.ShardingUpload.ProcessCount = 1
	transfer := new(model.Transfer)
	transfer.Src, transfer.Dst, transfer.Runtime = source, destination, runtime
	operation := new(model.TransferOperation)
	operation.OperationID = "operation"
	loader := tasks.NewSnapshotLoader(coordinator.NewStatefulFakeClient(), operation, transfer, solomon.NewRegistry(nil))
	table := new(abstract.TableDescription)
	table.Schema, table.Name = "source", "events"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		done <- loader.UploadTables(ctx, []abstract.TableDescription{*table}, false)
		close(finished)
	}()
	test.Cleanup(func() {
		releasePublish()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			test.Error("loader did not stop after releasing the publication fixture")
		}
	})
	select {
	case <-started:
	case err := <-done:
		test.Fatalf("loader stopped before committing the snapshot: %v", err)
	case <-time.After(5 * time.Second):
		test.Fatal("loader did not reach snapshot publication")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(test, err, context.Canceled)
	case <-time.After(2 * time.Second):
		test.Fatal("loader cancellation did not reach the snapshot commit")
	}
	record, err := sink.publicationStore.LoadTable(context.Background(), stateKey)
	require.NoError(test, err)
	require.NotNil(test, record)
	require.True(test, record.PublicationDecided())
	require.False(test, record.PublicationComplete())
	require.NotNil(test, record.Replicas()[0].PendingPublicationCommand)
	require.Equal(test, record.Replicas()[0].CandidateTable.ExpectedTableUUID, publication.destinationUUID)
	require.Equal(test, 1, publication.exchanges)

	releasePublish()
	recovery := newCommitRecoverySink(sink.publicationStore.ForOperation("operation"), sink.databaseOperations)
	require.NoError(test, recovery.Cleanup(context.Background()))
	record, err = recovery.publicationStore.LoadTable(context.Background(), stateKey)
	require.NoError(test, err)
	require.Nil(test, record)
	require.Equal(test, 1, publication.exchanges)
}
