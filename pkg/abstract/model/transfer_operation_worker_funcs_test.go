package model

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
)

type testCategorizedError struct {
	error
	category categories.Category
}

func (e *testCategorizedError) Unwrap() error {
	return e.error
}

func (e *testCategorizedError) Category() categories.Category {
	return e.category
}

func extractCode(t *testing.T, err error) coded.Code {
	var codedErr coded.CodedError
	require.True(t, xerrors.As(err, &codedErr))
	return codedErr.Code()
}

func extractCategory(t *testing.T, err error) categories.Category {
	var categorized categories.CategorizedError
	require.True(t, xerrors.As(err, &categorized))
	return categorized.Category()
}

func TestAggregateWorkerErrorsNoErrors(t *testing.T) {
	workers := []*OperationWorker{
		{OperationID: "op-1", WorkerIndex: 1, Completed: true},
		{OperationID: "op-1", WorkerIndex: 2, Completed: false},
	}
	require.NoError(t, AggregateWorkerErrors(workers, "op-1"))
}

func TestAggregateWorkerErrorsUncodedStaysUncoded(t *testing.T) {
	workers := []*OperationWorker{
		{OperationID: "op-1", WorkerIndex: 1, Err: "boom-1"},
		{OperationID: "op-1", WorkerIndex: 2},
		{OperationID: "op-1", WorkerIndex: 3, Err: "boom-3"},
	}
	err := AggregateWorkerErrors(workers, "op-1")
	require.Error(t, err)

	// an unmarked leaf must stay uncoded, so that it keeps showing up in top uncoded errors
	var codedErr coded.CodedError
	require.False(t, xerrors.As(err, &codedErr))

	require.Contains(t, err.Error(), "secondary worker [1] of operation 'op-1' failed: boom-1")
	require.Contains(t, err.Error(), "secondary worker [3] of operation 'op-1' failed: boom-3")
	require.NotContains(t, err.Error(), "secondary worker [2]")
}

func TestAggregateWorkerErrorsRestoresWorkerCodeAndCategory(t *testing.T) {
	workers := []*OperationWorker{
		{OperationID: "op-1", WorkerIndex: 1, Err: "boom-1"},
		{OperationID: "op-1", WorkerIndex: 2, Err: "boom-2", Code: error_codes.PostgresDDLApplyFailed.ID(), Categories: []string{categories.Source.ID()}},
	}
	err := AggregateWorkerErrors(workers, "op-1")
	require.Error(t, err)

	require.Equal(t, error_codes.PostgresDDLApplyFailed, extractCode(t, err))
	require.Equal(t, categories.Source, extractCategory(t, err))
}

func TestAggregateWorkerErrorsUnspecifiedCodeIsNotRestored(t *testing.T) {
	workers := []*OperationWorker{
		{OperationID: "op-1", WorkerIndex: 1, Err: "boom", Code: error_codes.Unspecified.ID()},
	}
	err := AggregateWorkerErrors(workers, "op-1")
	require.Error(t, err)

	var codedErr coded.CodedError
	require.False(t, xerrors.As(err, &codedErr))
}

func TestAggregateWorkerErrorsCodeSurvivesWrapping(t *testing.T) {
	workers := []*OperationWorker{
		{OperationID: "op-1", WorkerIndex: 1, Err: "boom", Code: error_codes.PostgresDDLApplyFailed.ID()},
	}
	err := AggregateWorkerErrors(workers, "op-1")
	require.Error(t, err)

	wrapped := xerrors.Errorf("errors detected on secondary workers: %w", err)
	require.Equal(t, error_codes.PostgresDDLApplyFailed, extractCode(t, wrapped))
}

func TestOperationWorkerSetErr(t *testing.T) {
	leaf := &testCategorizedError{error: xerrors.New("boom"), category: categories.Source}
	err := coded.New(error_codes.PostgresDDLApplyFailed, leaf)

	worker := NewOperationWorker()
	worker.SetErr(xerrors.Errorf("wrapped: %w", err))

	require.Equal(t, "wrapped: boom", worker.Err)
	require.Equal(t, error_codes.PostgresDDLApplyFailed.ID(), worker.Code)
	require.Equal(t, []string{categories.Source.ID()}, worker.Categories)
}

func TestOperationWorkerSetErrPlainError(t *testing.T) {
	worker := NewOperationWorker()
	worker.SetErr(xerrors.New("boom"))

	require.Equal(t, "boom", worker.Err)
	require.Equal(t, "", worker.Code)
	require.Empty(t, worker.Categories)
}
