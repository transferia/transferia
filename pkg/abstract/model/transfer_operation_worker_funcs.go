package model

import (
	"errors"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
)

type categorizedWorkerError struct {
	error
	category categories.Category
}

func (e *categorizedWorkerError) Unwrap() error {
	return e.error
}

func (e *categorizedWorkerError) Category() categories.Category {
	return e.category
}

// SetErr saves a failed worker's error, code and category from plain error.
func (w *OperationWorker) SetErr(err error) {
	w.Err = err.Error()
	var codedErr coded.CodedError
	if xerrors.As(err, &codedErr) {
		w.Code = codedErr.Code().ID()
	}
	var categorized categories.CategorizedError
	if xerrors.As(err, &categorized) {
		w.Categories = []string{categorized.Category().ID()}
	}
}

func AggregateWorkerErrors(workers []*OperationWorker, operationID string) error {
	var result []error
	for _, worker := range workers {
		if worker.Err == "" {
			continue
		}
		workerErr := xerrors.Errorf("secondary worker [%v] of operation '%v' failed: %v", worker.WorkerIndex, operationID, worker.Err)
		if len(worker.Categories) > 0 {
			workerErr = &categorizedWorkerError{error: workerErr, category: categories.Category(worker.Categories[0])}
		}
		if worker.Code != "" && worker.Code != error_codes.Unspecified.ID() {
			workerErr = coded.New(coded.Code(worker.Code), workerErr)
		}
		result = append(result, workerErr)
	}
	return errors.Join(result...)
}
