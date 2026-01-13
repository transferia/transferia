package tasks

import (
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
)

func defaultCheckAreWorkersDone(startTime time.Time, cp coordinator.Coordinator, operationID string, workersCount int) (bool, error) {
	totalProgress, err := cp.GetOperationProgress(operationID)
	if err != nil {
		return false, errors.CategorizedErrorf(categories.Internal, "can't to get progress for operation '%v': %w", operationID, err)
	}
	workers, err := cp.GetOperationWorkers(operationID)
	if err != nil {
		return false, errors.CategorizedErrorf(categories.Internal, "can't to get workers for operation '%v': %w", operationID, err)
	}

	if workersCount != len(workers) {
		return false, errors.CategorizedErrorf(categories.Internal, "expected workers count '%v' not equal real workers count '%v' for operation '%v'",
			workersCount, len(workers), operationID)
	}

	completedWorkersCount := 0
	partsInProgress := int64(0)
	progress := model.NewAggregatedProgress()
	progress.PartsCount = totalProgress.PartsCount
	progress.ETARowsCount = totalProgress.ETARowsCount
	for _, worker := range workers {
		if worker.Completed {
			completedWorkersCount++
		}

		if worker.Progress != nil {
			partsInProgress += worker.Progress.PartsCount - worker.Progress.CompletedPartsCount
			progress.CompletedPartsCount += worker.Progress.CompletedPartsCount
			progress.CompletedRowsCount += worker.Progress.CompletedRowsCount
		}
	}

	completedWorkersCountPercent := float64(0)
	if workersCount != 0 {
		completedWorkersCountPercent = (float64(completedWorkersCount) / float64(workersCount)) * 100
	}

	completed := (completedWorkersCount == workersCount)

	status := "running"
	if completed {
		status = "completed"
	}
	logger.Log.Infof(
		"Secondary workers are %v, workers: %v in progress, %v completed, %v total (%.2f%% completed), parts: %v in progress, %v completed, %v total (%.2f%% completed), rows: %v / %v (%.2f%%), elapsed time: %v",
		status,
		workersCount-completedWorkersCount,
		completedWorkersCount,
		workersCount,
		completedWorkersCountPercent,
		partsInProgress,
		progress.CompletedPartsCount,
		progress.PartsCount,
		progress.PartsPercent(),
		progress.CompletedRowsCount,
		progress.ETARowsCount,
		progress.RowsPercent(),
		time.Since(startTime),
	)

	errs := model.AggregateWorkerErrors(workers, operationID)
	if len(errs) > 0 {
		return false, xerrors.Errorf("errors detected on secondary workers: %v", errs)
	}

	if completed {
		return true, nil
	} else {
		return false, nil
	}
}
