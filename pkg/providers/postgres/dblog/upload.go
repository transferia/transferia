package dblog

import (
	"context"
	"fmt"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/sink_factory"
	"github.com/transferia/transferia/pkg/snapshot/progress_tracker"
	backoffutil "github.com/transferia/transferia/pkg/util/backoff"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
)

// UploadTables copies tables using DBLog and persists their snapshot progress.
// newStorage is called for each load attempt and may wrap the shared pgStorage.
// UploadTables closes neither storage; the caller manages their lifetime.
func UploadTables(
	ctx context.Context,
	tables []abstract.TableDescription,
	transfer *model.Transfer,
	task *model.TransferOperation,
	registry core_metrics.Registry,
	cp coordinator.Coordinator,
	pgStorage abstract.Storage,
	newStorage func(...StorageOption) (abstract.Storage, error),
) error {
	var operationID string
	if task != nil {
		operationID = task.OperationID
	}

	parts, err := prepareTableParts(tables, operationID, cp, pgStorage)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return nil
	}

	var progressMu sync.Mutex
	progressTracker := progress_tracker.NewTableTracker(cp, operationID, &progressMu, ctxlog.ContextFields(ctx))
	defer progressTracker.Close()

	for _, part := range parts {
		table := *part.ToTableDescription()
		progressTracker.Add(part)
		chunkCompletionCallback := func(rows uint64) {
			progressTracker.IncCompletedRows(part, rows)
		}
		asyncSink, err := sink_factory.MakeAsyncSink(
			transfer,
			task,
			logger.Log,
			registry,
			cp,
			middlewares.MakeConfig(middlewares.WithEnableRetries),
		)
		if err != nil {
			return xerrors.Errorf("failed to make async sink: %w", err)
		}

		pusher := abstract.PusherFromAsyncSink(asyncSink)
		if err := loadTableWithRetries(
			ctx,
			table,
			pusher,
			newStorage,
			WithOnProgressReset(func() { progressTracker.Start(part) }),
			WithChunkCompletionCallback(chunkCompletionCallback),
		); err != nil {
			return xerrors.Errorf("failed to load table: %w", err)
		}
		progressTracker.Complete(part)
		if err := progressTracker.Flush(true); err != nil {
			return xerrors.Errorf("failed to finalize table progress: %w", err)
		}
	}

	return nil
}

func prepareTableParts(
	tables []abstract.TableDescription,
	operationID string,
	cp coordinator.Coordinator,
	pgStorage abstract.Storage,
) ([]*abstract.OperationTablePart, error) {
	parts := make([]*abstract.OperationTablePart, 0, len(tables))
	for _, table := range tables {
		if abstract.IsSystemTable(table.Name) {
			continue
		}
		part := abstract.NewOperationTablePartFromDescription(operationID, &table)
		part.PartsCount = 1
		var err error
		part.ETARows, err = pgStorage.EstimateTableRowsCount(table.ID())
		if err != nil {
			logger.Log.Warn("dblog: failed to estimate rows count for progress tracking", log.String("table", table.Fqtn()), log.Error(err))
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return parts, nil
	}
	savedParts, err := cp.GetOperationTablesParts(operationID)
	if err != nil {
		return nil, xerrors.Errorf("failed to get saved table progress: %w", err)
	}
	completedRows := make(map[string]uint64, len(savedParts))
	for _, part := range savedParts {
		completedRows[part.Key()] = part.CompletedRows
	}
	partsToCreate := make([]*abstract.OperationTablePart, 0, len(parts))
	for _, part := range parts {
		if rows, ok := completedRows[part.Key()]; ok {
			part.CompletedRows = rows
		} else {
			partsToCreate = append(partsToCreate, part)
		}
	}
	if len(partsToCreate) > 0 {
		if err := cp.CreateOperationTablesParts(operationID, partsToCreate); err != nil {
			return nil, xerrors.Errorf("failed to register table parts for progress tracking: %w", err)
		}
	}

	return parts, nil
}

func loadTableWithRetries(
	ctx context.Context,
	table abstract.TableDescription,
	pusher abstract.Pusher,
	newStorage func(...StorageOption) (abstract.Storage, error),
	opts ...StorageOption,
) error {
	loadTable := func() error {
		logger.Log.Infof("Starting upload table: %s", table.String())

		dblogStorage, err := newStorage(opts...)
		if err != nil {
			return err
		}

		if err := dblogStorage.LoadTable(ctx, table, pusher); err != nil {
			return xerrors.Errorf("unable to dblogStorage.LoadTable, err: %w", err)
		}
		logger.Log.Infof("Upload table %s successfully", table.String())
		return nil
	}
	return backoff.RetryNotify(func() error {
		loadTableErr := loadTable()
		if abstract.IsFatal(loadTableErr) {
			loadTableErr = backoff.Permanent(loadTableErr)
		}
		return loadTableErr
	}, backoffutil.NewExponentialBackOff(), backoffutil.Log(ctx, fmt.Sprintf("loading table: %s", table.String())))
}
