package tasks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/base/adapter"
	"github.com/transferia/transferia/pkg/base/events"
	"github.com/transferia/transferia/pkg/base/filter"
	"github.com/transferia/transferia/pkg/data"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/sink"
	"github.com/transferia/transferia/pkg/storage"
	"github.com/transferia/transferia/pkg/targets"
	"github.com/transferia/transferia/pkg/targets/legacy"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/semaphore"
)

func (l *SnapshotLoader) descriptionsToParts(operationID string, descriptions ...abstract.TableDescription) []*model.OperationTablePart {
	tables := map[string][]abstract.TableDescription{}
	for _, description := range descriptions {
		tables[description.Fqtn()] = append(tables[description.Fqtn()], description)
	}

	var parts []*model.OperationTablePart
	for _, tableDescriptions := range tables {
		for i, description := range tableDescriptions {
			part := model.NewOperationTablePartFromDescription(operationID, &description)
			part.PartIndex = uint64(i)
			part.PartsCount = uint64(len(tableDescriptions))
			parts = append(parts, part)
		}
	}

	return parts
}

func (l *SnapshotLoader) sendTableControlEventV2(
	eventFactory func(part *model.OperationTablePart) (base.Event, error),
	target base.EventTarget,
	tables ...*model.OperationTablePart,
) error {
	tablesSet := map[string]bool{}
	for _, table := range tables {
		fqtn := table.TableFQTN()
		if tablesSet[fqtn] {
			continue
		}
		tablesSet[fqtn] = true

		event, err := eventFactory(table)
		if err != nil {
			return xerrors.Errorf("unable to build event: %w", err)
		}
		eventString := base.EventToString(event)

		if err := <-target.AsyncPush(base.NewEventBatch([]base.Event{event})); err != nil {
			return xerrors.Errorf("unable to sent '%v' for table %v: %w", eventString, fqtn, err)
		}

		logger.Log.Info(
			fmt.Sprintf("Sent control event '%v' for table %v", eventString, fqtn),
			log.String("event", eventString), log.String("table", fqtn))
	}
	return nil
}

func (l *SnapshotLoader) sendStateEventV2(state events.TableLoadState, provider base.SnapshotProvider, target base.EventTarget, tables ...*model.OperationTablePart) error {
	eventFactory := func(part *model.OperationTablePart) (base.Event, error) {
		dataObjectPart, err := provider.TablePartToDataObjectPart(part.ToTableDescription())
		if err != nil {
			return nil, xerrors.Errorf("unable create data object part for table part %v: %w", part.TableFQTN(), err)
		}
		cols, err := provider.TableSchema(dataObjectPart)
		if err != nil {
			return nil, xerrors.Errorf("unable to load table schema: %w", err)
		}
		return events.NewDefaultTableLoadEvent(adapter.NewTableFromLegacy(cols, *part.ToTableID()), state), nil
	}

	return l.sendTableControlEventV2(eventFactory, target, tables...)
}

func (l *SnapshotLoader) sendCleanupEventV2(target base.EventTarget, tables ...*model.OperationTablePart) error {
	eventFactory := func(part *model.OperationTablePart) (base.Event, error) {
		return events.CleanupEvent(*part.ToTableID()), nil
	}

	return l.sendTableControlEventV2(eventFactory, target, tables...)
}

func (l *SnapshotLoader) makeTargetV2(lgr log.Logger) (dataTarget base.EventTarget, closer func(), err error) {
	dataTarget, err = targets.NewTarget(l.transfer, lgr, l.registry, l.cp)
	if xerrors.Is(err, targets.UnknownTargetError) { // Legacy fallback
		legacySink, err := sink.MakeAsyncSink(l.transfer, lgr, l.registry, l.cp, middlewares.MakeConfig(middlewares.WithEnableRetries))
		if err != nil {
			return nil, nil, xerrors.Errorf("error creating legacy sink: %w", err)
		}
		dataTarget = legacy.NewEventTarget(lgr, legacySink, l.transfer.Dst.CleanupMode(), l.transfer.TmpPolicy)
	} else if err != nil {
		return nil, nil, xerrors.Errorf("unable to create target: %w", err)
	}

	closer = func() {
		if err := dataTarget.Close(); err != nil {
			lgr.Error("error closing EventTarget", log.Error(err))
		}
	}
	return dataTarget, closer, nil
}

func (l *SnapshotLoader) applyTransferTmpPolicyV2(inputFilter base.DataObjectFilter) error {
	if l.transfer.TmpPolicy == nil {
		return nil
	}

	if err := model.EnsureTmpPolicySupported(l.transfer.Dst, l.transfer); err != nil {
		return xerrors.Errorf(model.ErrInvalidTmpPolicy, err)
	}
	l.transfer.TmpPolicy = l.transfer.TmpPolicy.WithInclude(func(tableID abstract.TableID) bool {
		if inputFilter == nil {
			return true
		}
		ok, _ := inputFilter.IncludesID(tableID)
		return ok
	})
	return nil
}

func (l *SnapshotLoader) createSnapshotProviderV2() (base.SnapshotProvider, error) {
	snapshotProvider, err := data.NewSnapshotProvider(logger.Log, l.registry, l.transfer, l.cp)
	if err != nil {
		return nil, xerrors.Errorf("unable to create snapshot provider: %w", err)
	}
	if err := snapshotProvider.Ping(); err != nil {
		return nil, xerrors.Errorf("unable to ping data provider: %w", err)
	}
	if err := snapshotProvider.Init(); err != nil {
		return nil, xerrors.Errorf("unable to init data provider: %w", err)
	}
	return snapshotProvider, nil
}

func IntersectFilter(transfer *model.Transfer, basic base.DataObjectFilter) (base.DataObjectFilter, error) {
	if transfer.DataObjects == nil || len(transfer.DataObjects.IncludeObjects) == 0 {
		return basic, nil
	}
	transferFilter, err := filter.NewFromObjects(transfer.DataObjects.IncludeObjects)
	if err != nil {
		return nil, xerrors.Errorf("unable to construct filter from transfer objects: %w", err)
	}
	if basic != nil {
		return filter.NewIntersect(basic, transferFilter), nil
	}
	return transferFilter, nil
}

func (l *SnapshotLoader) UploadV2(ctx context.Context, snapshotProvider base.SnapshotProvider, tables []abstract.TableDescription) error {
	paralleledRuntime, ok := l.transfer.Runtime.(abstract.ShardingTaskRuntime)

	if !ok || paralleledRuntime.SnapshotWorkersNum() <= 1 {
		if err := l.uploadV2Single(ctx, snapshotProvider, tables); err != nil {
			return xerrors.Errorf("unable to upload tables: %w", err)
		}
		return nil
	}

	if err := l.uploadV2Sharded(ctx, snapshotProvider, tables); err != nil {
		return xerrors.Errorf("unable to sharded upload tables: %w", err)
	}
	return nil
}

func (l *SnapshotLoader) uploadV2Single(ctx context.Context, snapshotProvider base.SnapshotProvider, inTables []abstract.TableDescription) error {
	if inTables != nil && len(inTables) == 0 {
		return abstract.NewFatalError(xerrors.New("no tables in snapshot"))
	}

	var inputFilter base.DataObjectFilter
	if inTables != nil {
		inputFilter = filter.NewFromDescription(inTables)
	}

	if err := l.applyTransferTmpPolicyV2(inputFilter); err != nil {
		return xerrors.Errorf("failed apply transfer tmp policy: %w", err)
	}

	if hackable, ok := l.transfer.Dst.(model.HackableTarget); ok {
		hackable.PreSnapshotHacks()
	}

	if snapshotProvider == nil {
		var err error
		snapshotProvider, err = l.createSnapshotProviderV2()
		if err != nil {
			return xerrors.Errorf("unable to create snapshot provider: %w", err)
		}
	}

	dataTarget, closeTarget, err := l.makeTargetV2(logger.Log)
	if err != nil {
		return xerrors.Errorf("unable to create target: %w", err)
	}
	defer closeTarget()

	if err := snapshotProvider.BeginSnapshot(); err != nil {
		return xerrors.Errorf("unable to begin snapshot: %w", err)
	}

	sourceStorage, err := storage.NewStorage(l.transfer, l.cp, l.registry)
	if err != nil {
		return errors.CategorizedErrorf(categories.Source, ResolveStorageErrorText, err)
	}
	defer sourceStorage.Close()

	tables, nextIncrementalState, err := l.prepareIncrementalState(ctx, sourceStorage, inTables, true)
	if err != nil {
		return xerrors.Errorf("unable to prepare incremental state: %w", err)
	}

	if len(nextIncrementalState) != 0 {
		inputFilter = filter.NewFromDescription(tables)
	}

	composeFilter, err := IntersectFilter(l.transfer, inputFilter)
	if err != nil {
		return xerrors.Errorf("unable compose inputFilter: %w", err)
	}

	descriptions, err := snapshotProvider.DataObjectsToTableParts(composeFilter)
	if err != nil {
		return xerrors.Errorf("unable to get table parts: %w", err)
	}

	parts := l.descriptionsToParts(l.operationID, descriptions...)
	for _, table := range parts {
		table.WorkerIndex = new(int)
		*table.WorkerIndex = 0 // Because we have one worker
	}
	l.dumpTablePartsToLogs(parts)

	if err := l.cp.CreateOperationTablesParts(l.operationID, parts); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to store operation tables: %w", err)
	}

	if err := l.sendCleanupEventV2(dataTarget, parts...); err != nil {
		return xerrors.Errorf("unable cleanup tables: %w", err)
	}

	if err := l.sendStateEventV2(events.InitShardedTableLoad, snapshotProvider, dataTarget, parts...); err != nil {
		return xerrors.Errorf("unable to start loading tables: %w", err)
	}

	metricsTracker, err := NewNotShardedSnapshotTableMetricsTracker(ctx, l.transfer, l.registry, parts, &l.progressUpdateMutex)
	if err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to start metrics tracker: %w", err)
	}
	defer metricsTracker.Close()

	if err := l.doUploadTablesV2(ctx, snapshotProvider, l.GetLocalTablePartProvider(parts...)); err != nil {
		return xerrors.Errorf("unable to upload data objects: %w", err)
	}

	if err := l.sendStateEventV2(events.DoneShardedTableLoad, snapshotProvider, dataTarget, parts...); err != nil {
		return xerrors.Errorf("unable to start loading tables: %w", err)
	}

	if err := snapshotProvider.EndSnapshot(); err != nil {
		return xerrors.Errorf("unable to end snapshot: %w", err)
	}

	if err := l.endDestinationV2(); err != nil {
		logger.Log.Error("Failed to end snapshot on sink", log.Error(err))
		return xerrors.Errorf("failed to end snapshot on sink: %v", err)
	}

	if err := snapshotProvider.Close(); err != nil {
		return xerrors.Errorf("unable to close data provider: %w", err)
	}

	if err := l.setIncrementalState(nextIncrementalState); err != nil {
		logger.Log.Error("unable to set transfer state", log.Error(err))
	}
	logger.Log.Info("next incremental state uploaded", log.Any("state", nextIncrementalState))

	if hackable, ok := l.transfer.Dst.(model.HackableTarget); ok {
		hackable.PostSnapshotHacks()
	}

	return nil
}

func (l *SnapshotLoader) uploadV2Sharded(ctx context.Context, snapshotProvider base.SnapshotProvider, tables []abstract.TableDescription) error {
	if l.transfer.IsMain() {
		if err := l.uploadV2Main(ctx, snapshotProvider, tables); err != nil {
			return xerrors.Errorf("unable to sharded upload(main worker) tables v2: %w", err)
		}
		return nil
	}

	if err := l.uploadV2Secondary(ctx, snapshotProvider); err != nil {
		return xerrors.Errorf("unable to sharded upload(secondary worker) tables v2: %w", err)
	}
	return nil
}

func (l *SnapshotLoader) uploadV2Main(ctx context.Context, snapshotProvider base.SnapshotProvider, inTables []abstract.TableDescription) error {
	paralleledRuntime, ok := l.transfer.Runtime.(abstract.ShardingTaskRuntime)
	if !ok || paralleledRuntime.SnapshotWorkersNum() <= 1 {
		return errors.CategorizedErrorf(categories.Internal, "run sharding upload with non sharding runtime for operation '%v'", l.operationID)
	}

	if inTables != nil && len(inTables) == 0 {
		return abstract.NewFatalError(xerrors.New("no tables in snapshot"))
	}

	if l.transfer.TmpPolicy != nil {
		return abstract.NewFatalError(
			xerrors.Errorf("sharded transfer do not support temporary tables policy, please, turn it off or make transfer not sharded"))
	}

	var inputFilter base.DataObjectFilter
	if inTables != nil {
		inputFilter = filter.NewFromDescription(inTables)
	}

	if hackable, ok := l.transfer.Dst.(model.HackableTarget); ok {
		hackable.PreSnapshotHacks()
	}

	if snapshotProvider == nil {
		var err error
		snapshotProvider, err = l.createSnapshotProviderV2()
		if err != nil {
			return xerrors.Errorf("unable to create snapshot provider: %w", err)
		}
	}

	dataTarget, closeTarget, err := l.makeTargetV2(logger.Log)
	if err != nil {
		return xerrors.Errorf("unable to create target: %w", err)
	}
	defer closeTarget()

	if err := snapshotProvider.BeginSnapshot(); err != nil {
		return xerrors.Errorf("unable to begin snapshot: %w", err)
	}

	sourceStorage, err := storage.NewStorage(l.transfer, l.cp, l.registry)
	if err != nil {
		return errors.CategorizedErrorf(categories.Source, ResolveStorageErrorText, err)
	}
	defer sourceStorage.Close()

	tables, nextIncrementalState, err := l.prepareIncrementalState(ctx, sourceStorage, inTables, true)
	if err != nil {
		return xerrors.Errorf("unable to prepare incremental state: %w", err)
	}

	if len(nextIncrementalState) != 0 {
		inputFilter = filter.NewFromDescription(tables)
	}

	composeFilter, err := IntersectFilter(l.transfer, inputFilter)
	if err != nil {
		return xerrors.Errorf("unable compose inputFilter: %w", err)
	}

	descriptions, err := snapshotProvider.DataObjectsToTableParts(composeFilter)
	if err != nil {
		return xerrors.Errorf("unable to get table parts: %w", err)
	}

	parts := l.descriptionsToParts(l.operationID, descriptions...)
	l.dumpTablePartsToLogs(parts)

	if err := l.cp.CreateOperationTablesParts(l.operationID, parts); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to store operation tables: %w", err)
	}

	shardedState, err := l.MainWorkerCreateShardedStateFromSource(snapshotProvider)
	if err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to prepare sharded state for operation '%v': %w", l.operationID, err)
	}

	if err := l.SetShardedStateToCP(logger.Log, shardedState); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to set sharded state: %w", err)
	}

	metricsTracker, err := NewShardedSnapshotTableMetricsTracker(ctx, l.transfer, l.registry, l.operationID, l.cp)
	if err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to start metrics tracker: %w", err)
	}
	defer metricsTracker.Close()

	if err := l.sendCleanupEventV2(dataTarget, parts...); err != nil {
		return xerrors.Errorf("unable cleanup tables: %w", err)
	}

	if err := l.sendStateEventV2(events.InitShardedTableLoad, snapshotProvider, dataTarget, parts...); err != nil {
		return xerrors.Errorf("unable to start loading tables: %w", err)
	}

	if err := l.cp.CreateOperationWorkers(l.operationID, paralleledRuntime.SnapshotWorkersNum()); err != nil {
		return xerrors.Errorf("unable to create operation workers for operation '%v': %w", l.operationID, err)
	}

	if err := l.WaitWorkersCompleted(ctx, paralleledRuntime.SnapshotWorkersNum()); err != nil {
		return xerrors.Errorf("unable to wait shard completed: %w", err)
	}

	if err := l.sendStateEventV2(events.DoneShardedTableLoad, snapshotProvider, dataTarget, parts...); err != nil {
		return xerrors.Errorf("unable to start loading tables: %w", err)
	}

	if err := snapshotProvider.EndSnapshot(); err != nil {
		return xerrors.Errorf("unable to end snapshot: %w", err)
	}

	if err := l.endDestinationV2(); err != nil {
		logger.Log.Error("Failed to end snapshot on sink", log.Error(err))
		return xerrors.Errorf("failed to end snapshot on sink: %v", err)
	}

	if err := snapshotProvider.Close(); err != nil {
		return xerrors.Errorf("unable to close data provider: %w", err)
	}

	if err := l.setIncrementalState(nextIncrementalState); err != nil {
		logger.Log.Error("unable to set transfer state", log.Error(err))
	}
	logger.Log.Info("next incremental state uploaded", log.Any("state", nextIncrementalState))

	if hackable, ok := l.transfer.Dst.(model.HackableTarget); ok {
		hackable.PostSnapshotHacks()
	}

	return nil
}

func (l *SnapshotLoader) uploadV2Secondary(ctx context.Context, snapshotProvider base.SnapshotProvider) error {
	runtime, ok := l.transfer.Runtime.(abstract.ShardingTaskRuntime)
	if !ok || runtime.SnapshotWorkersNum() <= 1 {
		return errors.CategorizedErrorf(categories.Internal, "run sharding upload with non sharding runtime for operation '%v'", l.operationID)
	}

	if l.transfer.TmpPolicy != nil {
		return abstract.NewFatalError(
			xerrors.Errorf("sharded transfer do not support temporary tables policy, please, turn it off or make transfer not sharded"))
	}

	logger.Log.Infof("Sharding upload on worker '%v' started", l.workerIndex)

	shardedState, err := l.ReadFromCPShardState(ctx)
	if err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to get shard state: %w", err)
	}

	prevAssignedTablesParts, err := l.cp.ClearAssignedTablesParts(ctx, l.operationID, l.workerIndex)
	if err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable clear assigned tables parts for worker %v: %w", l.workerIndex, err)
	}
	if prevAssignedTablesParts > 0 {
		logger.Log.Warnf("Worker %v restarted, cleared assigned tables parts count %v", l.workerIndex, prevAssignedTablesParts)
	}

	if hackable, ok := l.transfer.Dst.(model.HackableTarget); ok {
		hackable.PreSnapshotHacks()
	}

	if snapshotProvider == nil {
		var err error
		snapshotProvider, err = l.createSnapshotProviderV2()
		if err != nil {
			return xerrors.Errorf("unable to create snapshot provider: %w", err)
		}
	}

	if err := l.SetShardedStateToSource(snapshotProvider, shardedState); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "can't set sharded state to storage: %w", err)
	}

	logger.Log.Infof("Start uploading tables on worker %v", l.workerIndex)

	if err := l.doUploadTablesV2(ctx, snapshotProvider, l.GetRemoteTablePartProvider()); err != nil {
		return xerrors.Errorf("unable to upload data objects: %w", err)
	}

	logger.Log.Infof("Done uploading tables on worker %v", l.workerIndex)

	if err := snapshotProvider.Close(); err != nil {
		return xerrors.Errorf("unable to close data provider: %w", err)
	}

	if hackable, ok := l.transfer.Dst.(model.HackableTarget); ok {
		hackable.PostSnapshotHacks()
	}

	return nil
}

func (l *SnapshotLoader) doUploadTablesV2(ctx context.Context, snapshotProvider base.SnapshotProvider, nextTablePartProvider TablePartProvider) error {
	ctx = util.ContextWithTimestamp(ctx, time.Now())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	parallelismSemaphore := semaphore.NewWeighted(int64(l.parallelismParams.ProcessCount))
	waitToComplete := sync.WaitGroup{}
	errorOnce := sync.Once{}
	var tableUploadErr error

	progressTracker := NewSnapshotTableProgressTracker(ctx, l.operationID, l.cp, &l.progressUpdateMutex)
	defer progressTracker.Close()

	for ctx.Err() == nil {
		if err := parallelismSemaphore.Acquire(ctx, 1); err != nil {
			logger.Log.Error("Failed to acquire semaphore to load next table", log.Any("worker_index", l.workerIndex), log.Error(err))
			continue
		}

		nextTablePart, err := nextTablePartProvider()
		if err != nil {
			logger.Log.Error("Unable to get next table to upload", log.Int("worker_index", l.workerIndex), log.Error(ctx.Err()))
			parallelismSemaphore.Release(1)
			return errors.CategorizedErrorf(categories.Internal, "unable to get next table to upload: %w", err)
		}
		if nextTablePart == nil {
			logger.Log.Info("There are no more parts to transfer", log.Int("worker_index", l.workerIndex))
			parallelismSemaphore.Release(1)
			break // No more tables to transfer
		}
		waitToComplete.Add(1)
		go func() {
			defer waitToComplete.Done()
			defer parallelismSemaphore.Release(1)

			upload := func() error {
				if ctx.Err() != nil {
					logger.Log.Warn(
						fmt.Sprintf("Context is canceled while upload table '%v'", nextTablePart),
						log.Any("table_part", nextTablePart), log.Int("worker_index", l.workerIndex), log.Error(ctx.Err()))
					return nil
				}

				logger.Log.Info(
					fmt.Sprintf("Start load table '%v'", nextTablePart.String()),
					log.Any("table_part", nextTablePart), log.Int("worker_index", l.workerIndex))

				l.progressUpdateMutex.Lock()
				nextTablePart.CompletedRows = 0
				nextTablePart.Completed = false
				l.progressUpdateMutex.Unlock()

				progressTracker.Add(nextTablePart)

				dataObjectPart, err := snapshotProvider.TablePartToDataObjectPart(nextTablePart.ToTableDescription())
				if err != nil {
					return xerrors.Errorf("unable create data object part for table part %v: %w", nextTablePart.TableFQTN(), err)
				}

				snapshotSource, err := snapshotProvider.CreateSnapshotSource(dataObjectPart)
				if err != nil {
					return xerrors.Errorf("unable create snapshot source for part %v: %w", dataObjectPart.FullName(), err)
				}

				dataTarget, closeTarget, err := l.makeTargetV2(logger.Log)
				if err != nil {
					return xerrors.Errorf("unable to create target: %w", err)
				}
				defer closeTarget()

				getProgress := func() {
					progress, err := snapshotSource.Progress()
					if err != nil {
						logger.Log.Warn("Unable to get progress from snapshot source", log.Error(err))
						return
					}

					nextTablePart.CompletedRows = progress.Current()

					// Report progress to logs
					logger.Log.Info(
						fmt.Sprintf("Load table '%v' progress %v / %v (%.2f%%)", nextTablePart, nextTablePart.CompletedRows, nextTablePart.ETARows, nextTablePart.CompletedPercent()),
						log.Any("table_part", nextTablePart), log.Int("worker_index", l.workerIndex))
				}

				// Run background `nextTablePart.CompletedRows` updater.
				updaterCtx, cancelUpdater := context.WithCancel(context.Background())
				defer cancelUpdater()
				go func() {
					ticker := time.NewTicker(15 * time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-updaterCtx.Done():
							getProgress() // Final call.
							return
						case <-ticker.C:
							getProgress()
						}
					}
				}()

				if err := snapshotSource.Start(ctx, dataTarget); err != nil {
					return xerrors.Errorf("unable upload part %v: %w", dataObjectPart.FullName(), err)
				}

				cancelUpdater()

				l.progressUpdateMutex.Lock()
				nextTablePart.Completed = true
				l.progressUpdateMutex.Unlock()
				progressTracker.Flush()

				logger.Log.Info(
					fmt.Sprintf("Finish load table '%v' progress %v / %v (%.2f%%)", nextTablePart, nextTablePart.CompletedRows, nextTablePart.ETARows, nextTablePart.CompletedPercent()),
					log.Any("table_part", nextTablePart), log.Int("worker_index", l.workerIndex))

				return nil
			}

			b := backoff.WithMaxRetries(backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(0)), 3)
			operation := func() error {
				uploadErr := upload()
				if abstract.IsFatal(uploadErr) {
					return xerrors.Errorf("fatal error on part upload: %w", backoff.Permanent(uploadErr))
				}
				return uploadErr
			}
			if err := backoff.Retry(operation, b); err != nil {
				logger.Log.Error(
					fmt.Sprintf("Upload table '%v' max retries exceeded", nextTablePart),
					log.Any("table_part", nextTablePart), log.Int("worker_index", l.workerIndex), log.Error(err))
				cancel()
				errorOnce.Do(func() { tableUploadErr = err })
			}
		}()

	}
	waitToComplete.Wait()

	if tableUploadErr != nil {
		return errors.CategorizedErrorf(categories.Internal, "Upload error: %w", tableUploadErr)
	}

	return nil
}

func (l *SnapshotLoader) endDestinationV2() error {
	target, err := targets.NewTarget(l.transfer, logger.Log, l.registry, l.cp)
	if xerrors.Is(err, targets.UnknownTargetError) {
		return l.endDestination()
	}
	if err != nil {
		return xerrors.Errorf("unable to create target to try to end destination: %w", err)
	}
	if err := target.Close(); err != nil {
		return xerrors.Errorf("unable to close target on ending destination: %w", err)
	}

	return nil
}
