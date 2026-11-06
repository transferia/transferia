package progress

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	postgres_dblog "github.com/transferia/transferia/pkg/providers/postgres/dblog"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	Source = *pgrecipe.RecipeSource()

	testTableName = "__test_num_table"
	rowsCount     = uint64(10)
	chunkSize     = uint64(5)
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &model.MockDestination{}, abstract.TransferTypeSnapshotOnly)
	Source.DBLogEnabled = true
	Source.ChunkSize = chunkSize
}

// recordingCoordinator wraps CoordinatorNoOp and records progress-related calls.
type recordingCoordinator struct {
	*coordinator.CoordinatorNoOp

	mu             sync.Mutex
	createErr      error
	getErr         error
	discardUpdates bool
	parts          map[string]*abstract.OperationTablePart
	createdParts   []*abstract.OperationTablePart
	updatedParts   []*abstract.OperationTablePart
}

func (r *recordingCoordinator) CreateOperationTablesParts(operationID string, tables []*abstract.OperationTablePart) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.createErr != nil {
		return r.createErr
	}
	if r.parts == nil {
		r.parts = make(map[string]*abstract.OperationTablePart)
	}
	for _, t := range tables {
		r.parts[t.Key()] = t.Copy()
		r.createdParts = append(r.createdParts, t.Copy())
	}
	return nil
}

func (r *recordingCoordinator) UpdateOperationTablesParts(operationID string, tables []*abstract.OperationTablePart) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.discardUpdates {
		return nil
	}
	for _, t := range tables {
		r.parts[t.Key()] = t.Copy()
		r.updatedParts = append(r.updatedParts, t.Copy())
	}
	return nil
}

func (r *recordingCoordinator) GetOperationTablesParts(operationID string) ([]*abstract.OperationTablePart, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.getErr != nil {
		return nil, r.getErr
	}
	parts := make([]*abstract.OperationTablePart, 0, len(r.parts))
	for _, part := range r.parts {
		parts = append(parts, part.Copy())
	}
	return parts, nil
}

func TestDBLogProgressTracking(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	transferID := transferhelpers.GenerateTransferID("TestDBLogProgressTracking")
	source := Source
	source.SlotID = transferID

	require.NoError(t, provider_postgres.CreateReplicationSlot(&source))
	t.Cleanup(func() { require.NoError(t, provider_postgres.DropReplicationSlot(&source)) })

	cp := &recordingCoordinator{CoordinatorNoOp: coordinator.NewFakeClient()}
	destination := &model.MockDestination{SinkerFactory: func() abstract.Sinker {
		return mocksink.NewMockSink(func([]abstract.ChangeItem) error { return nil })
	}}
	transfer := transferhelpers.MakeTransfer(transferID, &source, destination, abstract.TransferTypeSnapshotOnly)

	pgProvider := provider_postgres.New(logger.Log, testmetrics.EmptyRegistry(), cp, transfer, nil).(*provider_postgres.Provider)

	pgStorage, err := provider_postgres.NewStorage(source.ToStorageParams(nil))
	require.NoError(t, err)
	defer pgStorage.Close()

	tables, err := pgStorage.TableList(nil)
	require.NoError(t, err)

	task := &model.TransferOperation{OperationID: transferID}
	cp.createErr = errors.New("register parts failed")
	require.NoError(t, pgProvider.DBLogUpload(context.Background(), nil, task))
	require.ErrorIs(t, pgProvider.DBLogUpload(context.Background(), tables, task), cp.createErr)
	require.ErrorIs(t, pgProvider.DBLogUpload(context.Background(), tables, nil), cp.createErr)
	require.Empty(t, cp.createdParts)
	require.Empty(t, cp.updatedParts)
	cp.createErr = nil
	require.NoError(t, pgProvider.DBLogUpload(context.Background(), tables, task))

	cp.mu.Lock()
	defer cp.mu.Unlock()

	// CreateOperationTablesParts must have been called for __test_num_table with a positive ETA.
	var createdPart *abstract.OperationTablePart
	for _, part := range cp.createdParts {
		if part.Name == testTableName {
			createdPart = part
			break
		}
	}
	require.NotNil(t, createdPart, "expected OperationTablePart for %s to be registered", testTableName)
	require.Greater(t, createdPart.ETARows, uint64(0))

	// The final UpdateOperationTablesParts call must carry Completed=true and CompletedRows=rowsCount.
	var finalPart *abstract.OperationTablePart
	for _, part := range cp.updatedParts {
		if part.Name == testTableName && part.Completed {
			finalPart = part
		}
	}
	require.NotNil(t, finalPart, "expected completed OperationTablePart for %s", testTableName)
	require.Equal(t, rowsCount, finalPart.CompletedRows)
}

func TestDBLogProgressRestoredAfterCrash(t *testing.T) {
	pgStorage, err := provider_postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)
	defer pgStorage.Close()
	tables, err := pgStorage.TableList(nil)
	require.NoError(t, err)
	tableID := abstract.TableID{Namespace: "public", Name: testTableName}
	require.Contains(t, tables, tableID)
	tables = abstract.TableMap{tableID: tables[tableID]}
	transferIDPrefix := transferhelpers.GenerateTransferID(t.Name())

	for _, tc := range []struct {
		name            string
		discardProgress bool
		missingPart     bool
		savedRows       uint64
	}{
		{name: "saved", savedRows: chunkSize},
		{name: "stale", discardProgress: true},
		{name: "missing", discardProgress: true, missingPart: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transferID := transferIDPrefix + "_" + tc.name
			source := Source
			source.SlotID = transferID
			require.NoError(t, provider_postgres.CreateReplicationSlot(&source))
			t.Cleanup(func() { require.NoError(t, provider_postgres.DropReplicationSlot(&source)) })

			// Discard Close's flush as well to model a crash before coordinator progress is saved.
			cp := &recordingCoordinator{CoordinatorNoOp: coordinator.NewFakeClient(), discardUpdates: tc.discardProgress}
			task := &model.TransferOperation{OperationID: transferID}
			pushErr := errors.New("sink failed after the first chunk")
			var pushedRows atomic.Uint64
			upload := func(fail bool) error {
				destination := &model.MockDestination{SinkerFactory: func() abstract.Sinker {
					return mocksink.NewMockSink(func(items []abstract.ChangeItem) error {
						var rows uint64
						for _, item := range items {
							if item.TableID() == tableID && item.Kind == abstract.InsertKind && item.ID == 0 {
								rows++
							}
						}
						if fail && rows > 0 && pushedRows.Load() >= chunkSize {
							// Stop both retry loops without marking the error fatal: fatal errors delete the replication slot.
							return abstract.NewTableUploadError(backoff.Permanent(pushErr))
						}
						pushedRows.Add(rows)
						return nil
					})
				}}
				transfer := transferhelpers.MakeTransfer(transferID, &source, destination, abstract.TransferTypeSnapshotOnly)
				pgProvider := provider_postgres.New(logger.Log, testmetrics.EmptyRegistry(), cp, transfer, nil).(*provider_postgres.Provider)
				return pgProvider.DBLogUpload(context.Background(), tables, task)
			}

			require.ErrorIs(t, upload(true), pushErr)
			require.Equal(t, chunkSize, pushedRows.Load())
			savedParts, err := cp.GetOperationTablesParts(task.OperationID)
			require.NoError(t, err)
			require.Len(t, savedParts, 1)
			require.Equal(t, tc.savedRows, savedParts[0].CompletedRows)
			require.False(t, savedParts[0].Completed)
			require.Len(t, cp.createdParts, 1)

			cp.getErr = errors.New("saved progress unavailable")
			require.ErrorIs(t, upload(false), cp.getErr)
			require.Len(t, cp.createdParts, 1, "failed progress recovery must not overwrite saved parts")
			cp.getErr = nil
			parts, err := cp.GetOperationTablesParts(task.OperationID)
			require.NoError(t, err)
			require.Equal(t, savedParts, parts)

			if tc.missingPart {
				cp.parts = nil
			}
			cp.discardUpdates = false
			require.NoError(t, upload(false))
			registeredParts := 1
			if tc.missingPart {
				registeredParts++
			}
			require.Len(t, cp.createdParts, registeredParts, "restart must only register missing operation parts")
			parts, err = cp.GetOperationTablesParts(task.OperationID)
			require.NoError(t, err)
			require.Len(t, parts, 1)
			require.True(t, parts[0].Completed)
			require.Equal(t, tc.savedRows+rowsCount-chunkSize, parts[0].CompletedRows, "unsaved progress is not recovered from the watermark")
			require.Equal(t, rowsCount, pushedRows.Load(), "restart must resume copying from the saved watermark")

			if tc.discardProgress {
				return
			}
			require.NoError(t, postgres_dblog.DeleteWatermarks(context.Background(), pgStorage.Conn, source.KeeperSchema, transferID))
			pushedRows.Store(0)
			require.ErrorIs(t, upload(true), pushErr)
			parts, err = cp.GetOperationTablesParts(task.OperationID)
			require.NoError(t, err)
			require.Len(t, parts, 1)
			require.False(t, parts[0].Completed)
			require.Equal(t, chunkSize, parts[0].CompletedRows, "reading from the beginning must reset the old progress")
			require.Equal(t, chunkSize, pushedRows.Load())
		})
	}
}

func TestDBLogProgressPartsRegisteredOnce(t *testing.T) {
	transferID := transferhelpers.GenerateTransferID(t.Name())
	source := Source
	source.SlotID = transferID
	require.NoError(t, provider_postgres.CreateReplicationSlot(&source))
	t.Cleanup(func() { require.NoError(t, provider_postgres.DropReplicationSlot(&source)) })

	pgStorage, err := provider_postgres.NewStorage(source.ToStorageParams(nil))
	require.NoError(t, err)
	defer pgStorage.Close()
	tables, err := pgStorage.TableList(nil)
	require.NoError(t, err)
	tableID := abstract.TableID{Namespace: "public", Name: testTableName}
	require.Contains(t, tables, tableID)
	tables = abstract.TableMap{tableID: tables[tableID]}

	cp := coordinator.NewStatefulFakeClient()
	destination := &model.MockDestination{SinkerFactory: func() abstract.Sinker {
		return mocksink.NewMockSink(func([]abstract.ChangeItem) error { return nil })
	}}
	transfer := transferhelpers.MakeTransfer(transferID, &source, destination, abstract.TransferTypeSnapshotOnly)
	pgProvider := provider_postgres.New(logger.Log, testmetrics.EmptyRegistry(), cp, transfer, nil).(*provider_postgres.Provider)
	task := &model.TransferOperation{OperationID: transferID}
	for range 3 {
		require.NoError(t, pgProvider.DBLogUpload(context.Background(), tables, task))
		parts, err := cp.GetOperationTablesParts(task.OperationID)
		require.NoError(t, err)
		require.Len(t, parts, 1, "restart must not create duplicate operation parts")
		require.True(t, parts[0].Completed)
		require.Equal(t, rowsCount, parts[0].CompletedRows)
	}
}

func TestDBLogProgressInvalidLowBound(t *testing.T) {
	ctx := context.Background()
	pgStorage, err := provider_postgres.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)
	defer pgStorage.Close()
	transferID := transferhelpers.GenerateTransferID(t.Name())
	schema := "invalid_bound_" + transferID
	_, err = pgStorage.Conn.Exec(ctx, `CREATE SCHEMA "`+schema+`"`)
	require.NoError(t, err)
	defer func() {
		_, err := pgStorage.Conn.Exec(ctx, `DROP SCHEMA "`+schema+`" CASCADE`)
		require.NoError(t, err)
	}()
	_, err = postgres_dblog.NewPgSignalTable(ctx, pgStorage.Conn, logger.Log, transferID, schema)
	require.NoError(t, err)
	signalTableName := `"` + schema + `"."` + postgres_dblog.SignalTableName + `"`
	_, err = pgStorage.Conn.Exec(ctx, `INSERT INTO `+signalTableName+` (table_schema, table_name, transfer_id, mark_type, low_bound)
		VALUES ('public', $1, $2, 'S', 'invalid')`, testTableName, transferID)
	require.NoError(t, err)

	storage, err := postgres_dblog.NewStorage(logger.Log, nil, pgStorage, pgStorage.Conn, chunkSize, transferID, schema, provider_postgres.Represent,
		postgres_dblog.WithOnProgressReset(func() { t.Error("invalid low bound must not reset progress") }))
	require.NoError(t, err)
	err = storage.LoadTable(ctx, abstract.TableDescription{Schema: "public", Name: testTableName}, nil)
	require.ErrorContains(t, err, "failed to decode low bound")
	require.False(t, abstract.IsFatal(err), "invalid low bound must not delete the replication slot")

	destination := &model.MockDestination{SinkerFactory: func() abstract.Sinker {
		return mocksink.NewMockSink(func([]abstract.ChangeItem) error { return nil })
	}}
	transfer := transferhelpers.MakeTransfer(transferID, &Source, destination, abstract.TransferTypeSnapshotOnly)
	attempts := 0
	newStorage := func(opts ...postgres_dblog.StorageOption) (abstract.Storage, error) {
		attempts++
		if attempts > 1 {
			return nil, backoff.Permanent(errors.New("invalid low bound was retried"))
		}
		return postgres_dblog.NewStorage(logger.Log, nil, pgStorage, pgStorage.Conn, chunkSize, transferID, schema, provider_postgres.Represent, opts...)
	}
	err = postgres_dblog.UploadTables(ctx, []abstract.TableDescription{{Schema: "public", Name: testTableName}}, transfer,
		&model.TransferOperation{OperationID: transferID}, testmetrics.EmptyRegistry(), coordinator.NewStatefulFakeClient(), pgStorage, newStorage)
	require.ErrorContains(t, err, "failed to decode low bound")
	require.Equal(t, 1, attempts, "invalid low bound must stop the upload without retries")
}
