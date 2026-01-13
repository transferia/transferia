package model

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
)

func AggregateWorkerErrors(workers []*OperationWorker, operationID string) util.Errors {
	var result util.Errors
	for _, worker := range workers {
		if worker.Err != "" {
			result = append(result, xerrors.Errorf("secondary worker [%v] of operation '%v' failed: %v", worker.WorkerIndex, operationID, worker.Err))
		}
	}
	return result
}
