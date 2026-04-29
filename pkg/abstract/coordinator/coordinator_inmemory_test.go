package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

func TestCoordinatorInMemory_CreateOperationWorkers_InvalidCount(t *testing.T) {
	cp := NewStatefulFakeClient()
	require.Error(t, cp.CreateOperationWorkers("op1", 1))
	require.Error(t, cp.CreateOperationWorkers("op1", 0))
	require.NoError(t, cp.CreateOperationWorkers("op1", 2))
}

func TestCoordinatorInMemory_AssignOperationTablePart_PerWorker(t *testing.T) {
	cp := NewStatefulFakeClient()
	const op = "op-parts"
	require.NoError(t, cp.CreateOperationTablesParts(op, []*abstract.OperationTablePart{
		{Schema: "s", Name: "a", Offset: 0, Filter: "f1", ETARows: 10},
		{Schema: "s", Name: "b", Offset: 0, Filter: "f2", ETARows: 20},
	}))

	p0, err := cp.AssignOperationTablePart(op, 0)
	require.NoError(t, err)
	require.NotNil(t, p0)
	require.NotNil(t, p0.WorkerIndex)
	require.Equal(t, 0, *p0.WorkerIndex)

	p1, err := cp.AssignOperationTablePart(op, 1)
	require.NoError(t, err)
	require.NotNil(t, p1)
	require.Equal(t, 1, *p1.WorkerIndex)

	pNil, err := cp.AssignOperationTablePart(op, 2)
	require.NoError(t, err)
	require.Nil(t, pNil)
}

func TestCoordinatorInMemory_ClearAssignedTablesParts(t *testing.T) {
	cp := NewStatefulFakeClient()
	const op = "op-clear"
	require.NoError(t, cp.CreateOperationTablesParts(op, []*abstract.OperationTablePart{
		{Schema: "s", Name: "t", Offset: 0, Filter: "x", ETARows: 5},
	}))
	_, err := cp.AssignOperationTablePart(op, 1)
	require.NoError(t, err)

	n, err := cp.ClearAssignedTablesParts(context.Background(), op, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	pAgain, err := cp.AssignOperationTablePart(op, 1)
	require.NoError(t, err)
	require.NotNil(t, pAgain)
}

func TestCoordinatorInMemory_GetOperationWorkersCount(t *testing.T) {
	cp := NewStatefulFakeClient()
	const op = "op-wc"
	require.NoError(t, cp.CreateOperationWorkers(op, 2))

	c0, err := cp.GetOperationWorkersCount(op, false)
	require.NoError(t, err)
	require.Equal(t, 2, c0)

	c1, err := cp.GetOperationWorkersCount(op, true)
	require.NoError(t, err)
	require.Equal(t, 0, c1)

	require.NoError(t, cp.FinishOperation(op, "", "", 1, nil))

	c2, err := cp.GetOperationWorkersCount(op, true)
	require.NoError(t, err)
	require.Equal(t, 1, c2)
}

func TestCoordinatorInMemory_FinishOperation_ErrorAggregate(t *testing.T) {
	cp := NewStatefulFakeClient()
	const op = "op-err"
	require.NoError(t, cp.CreateOperationWorkers(op, 2))
	require.NoError(t, cp.FinishOperation(op, "", "", 1, xerrors.New("boom")))

	workers, err := cp.GetOperationWorkers(op)
	require.NoError(t, err)
	err = model.AggregateWorkerErrors(workers, op)
	require.Error(t, err)
}

func TestCoordinatorInMemory_GetOperationProgress(t *testing.T) {
	cp := NewStatefulFakeClient()
	const op = "op-prog"
	require.NoError(t, cp.CreateOperationTablesParts(op, []*abstract.OperationTablePart{
		{Schema: "s", Name: "t", Offset: 0, Filter: "", ETARows: 100},
	}))
	_, err := cp.AssignOperationTablePart(op, 0)
	require.NoError(t, err)
	require.NoError(t, cp.UpdateOperationTablesParts(op, []*abstract.OperationTablePart{
		{Schema: "s", Name: "t", Offset: 0, Filter: "", ETARows: 100, CompletedRows: 100, Completed: true},
	}))

	ag, err := cp.GetOperationProgress(op)
	require.NoError(t, err)
	require.Equal(t, int64(1), ag.PartsCount)
	require.Equal(t, int64(1), ag.CompletedPartsCount)
	require.Equal(t, int64(100), ag.ETARowsCount)
	require.Equal(t, int64(100), ag.CompletedRowsCount)
}

func TestCoordinatorInMemory_OperationHealth(t *testing.T) {
	cp := NewStatefulFakeClient()
	ts := time.Unix(1700000000, 0).UTC()
	require.NoError(t, cp.OperationHealth(context.Background(), "op-h", 2, ts))
}
