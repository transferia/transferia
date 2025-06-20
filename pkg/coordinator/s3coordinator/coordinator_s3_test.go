//go:build !disable_s3_coordinator

package s3coordinator

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
)

func TestCoordinatorS3TransferState(t *testing.T) {
	cp, err := NewS3Recipe(os.Getenv("S3_BUCKET"))
	require.NoError(t, err)
	transferID := "test-transfer"
	// Run individual tests
	t.Run("SetTransferState", func(t *testing.T) {
		stateData := map[string]*coordinator.TransferStateData{
			"file1": {Generic: "1"},
			"file2": {Generic: "2"},
		}
		err := cp.SetTransferState(transferID, stateData)
		require.NoError(t, err)
	})

	t.Run("GetTransferState", func(t *testing.T) {
		state, err := cp.GetTransferState(transferID)
		require.NoError(t, err)
		expectedData := map[string]*coordinator.TransferStateData{
			"file1": {Generic: "1"},
			"file2": {Generic: "2"},
		}
		require.Equal(t, expectedData, state)
	})

	t.Run("SetTransferState", func(t *testing.T) {
		stateData := map[string]*coordinator.TransferStateData{
			"file3": {Generic: "3"},
		}
		err := cp.SetTransferState(transferID, stateData)
		require.NoError(t, err)
	})

	t.Run("SetTransferState2", func(t *testing.T) {
		stateData := map[string]*coordinator.TransferStateData{
			"file1": {Generic: "11"},
		}
		err := cp.SetTransferState(transferID, stateData)
		require.NoError(t, err)
	})

	t.Run("RemoveTransferState", func(t *testing.T) {
		err := cp.RemoveTransferState(transferID, []string{"file2"})
		require.NoError(t, err)
	})

	t.Run("GetTransferState", func(t *testing.T) {
		state, err := cp.GetTransferState(transferID)
		require.NoError(t, err)
		expectedData := map[string]*coordinator.TransferStateData{
			"file1": {Generic: "11"},
			"file3": {Generic: "3"},
		}
		require.Equal(t, expectedData, state)
	})
}

func TestDataplaneServiceShardedTasks(t *testing.T) {
	dp, err := NewS3Recipe(os.Getenv("S3_BUCKET"))
	require.NoError(t, err)
	ctx := context.Background()
	operationID := "dtfsometask"

	// Step 1: Create workers and table parts
	require.NoError(t, dp.CreateOperationWorkers(operationID, 3))
	require.NoError(t, dp.CreateOperationTablesParts(operationID, []*model.OperationTablePart{
		{OperationID: operationID, Schema: "Schema", Name: "table 1", ETARows: 10},
		{OperationID: operationID, Schema: "Schema", Name: "table 2", ETARows: 20},
		{OperationID: operationID, Schema: "Schema", Name: "table 3", ETARows: 30},
		{OperationID: operationID, Schema: "Schema", Name: "table 4", ETARows: 40},
	}))

	// Step 2: Subtests

	t.Run("AssignTablesToWorkers", func(t *testing.T) {
		expectedAssignments := []struct {
			workerIndex int
			tableName   string
		}{
			{1, "table 1"},
			{2, "table 2"},
			{3, "table 3"},
			{3, "table 4"}, // worker 3 gets two assignments
		}

		for _, assign := range expectedAssignments {
			assigned, err := dp.AssignOperationTablePart(operationID, assign.workerIndex)
			require.NoError(t, err)
			require.NotNil(t, assigned.WorkerIndex)
			require.Equal(t, assign.workerIndex, *assigned.WorkerIndex)
			require.Equal(t, assign.tableName, assigned.Name)
		}
	})

	t.Run("ClearAssignedTablesParts", func(t *testing.T) {
		expectedClearedParts := []struct {
			workerIndex   int
			expectedCount int64
		}{
			{1, 1}, // Worker 1 has 1 table part to clear
			{2, 1}, // Worker 2 has 1 table part to clear
			{3, 2}, // Worker 3 has 2 table parts to clear
		}

		for _, clear := range expectedClearedParts {
			clearedCount, err := dp.ClearAssignedTablesParts(ctx, operationID, clear.workerIndex)
			require.NoError(t, err)
			require.Equal(t, clear.expectedCount, clearedCount)
		}
	})

	t.Run("ReassignTablesToWorkers", func(t *testing.T) {
		expectedAssignments := []struct {
			workerIndex int
			tableName   string
		}{
			{1, "table 1"},
			{2, "table 2"},
			{3, "table 3"},
			{3, "table 4"}, // worker 3 gets two assignments
		}

		for _, assign := range expectedAssignments {
			assigned, err := dp.AssignOperationTablePart(operationID, assign.workerIndex)
			require.NoError(t, err)
			require.NotNil(t, assigned.WorkerIndex)
			require.Equal(t, assign.workerIndex, *assigned.WorkerIndex)
			require.Equal(t, assign.tableName, assigned.Name)
		}
	})

	t.Run("UpdateAndValidateTableProgress", func(t *testing.T) {
		// Update table progress
		err := dp.UpdateOperationTablesParts(operationID, []*model.OperationTablePart{
			{OperationID: operationID, Schema: "Schema", Name: "table 1", ETARows: 10, CompletedRows: 5},
			{OperationID: operationID, Schema: "Schema", Name: "table 2", ETARows: 20, CompletedRows: 10},
			{OperationID: operationID, Schema: "Schema", Name: "table 3", ETARows: 30, CompletedRows: 15, Completed: true},
			{OperationID: operationID, Schema: "Schema", Name: "table 4", ETARows: 40, CompletedRows: 20, Completed: true},
		})
		require.NoError(t, err)

		// Validate updated table progress
		tables, err := dp.GetOperationTablesParts(operationID)
		require.NoError(t, err)
		require.Equal(t, 4, len(tables))

		expectedTables := []struct {
			name          string
			workerIndex   int
			etaRows       uint64
			completedRows uint64
			completed     bool
		}{
			{"table 1", 1, 10, 5, false},
			{"table 2", 2, 20, 10, false},
			{"table 3", 3, 30, 15, true},
			{"table 4", 3, 40, 20, true},
		}

		for i, expected := range expectedTables {
			require.Equal(t, expected.name, tables[i].Name)
			require.Equal(t, expected.workerIndex, *tables[i].WorkerIndex)
			require.Equal(t, expected.etaRows, tables[i].ETARows)
			require.Equal(t, expected.completedRows, tables[i].CompletedRows)
			require.Equal(t, expected.completed, tables[i].Completed)
		}
	})

	t.Run("ValidateWorkerCompletion", func(t *testing.T) {
		require.NoError(t, dp.FinishOperation(operationID, "", 1, nil))
		workers, err := dp.GetOperationWorkers(operationID)
		require.NoError(t, err)
		require.Equal(t, 3, len(workers))

		expectedCompletion := []bool{true, false, false}

		for i, worker := range workers {
			require.Equal(t, expectedCompletion[i], worker.Completed)
		}
	})
}
