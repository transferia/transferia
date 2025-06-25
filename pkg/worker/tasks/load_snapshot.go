package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers/greenplum"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/sink"
	"github.com/transferia/transferia/pkg/storage"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/semaphore"
)

const (
	ResolveStorageErrorText string = "failed to resolve storage: %w"
	TableListErrorText      string = "failed to list tables and their schemas: %w"

	asyncPartsDefaultBatchSize = 1000
	asyncPartsStateKeyPrefix   = "async-part"
)

type TablePartProvider func(context.Context) (*model.OperationTablePart, error)

type SnapshotLoader struct {
	cp          coordinator.Coordinator
	operationID string
	transfer    *model.Transfer
	registry    metrics.Registry

	cancelUpload context.CancelFunc
	waitErrCh    chan error

	// Transfer params
	parallelismParams *abstract.ShardUploadParams
	workerIndex       int

	// Snapshot state
	slotKiller             abstract.SlotKiller
	slotKillerErrorChannel <-chan error

	// Progress, metrics
	progressUpdateMutex sync.Mutex

	schemaCache map[abstract.TableID]*abstract.TableSchema
	schemaLock  sync.Mutex
}

func NewSnapshotLoader(cp coordinator.Coordinator, operationID string, transfer *model.Transfer, registry metrics.Registry) *SnapshotLoader {
	return &SnapshotLoader{
		cp:          cp,
		operationID: operationID,
		transfer:    transfer,
		registry:    registry,

		cancelUpload: nil,
		waitErrCh:    make(chan error),

		parallelismParams: transfer.ParallelismParams(),
		workerIndex:       transfer.CurrentJobIndex(),

		slotKiller:             abstract.MakeStubSlotKiller(),
		slotKillerErrorChannel: make(<-chan error),

		progressUpdateMutex: sync.Mutex{},

		schemaCache: make(map[abstract.TableID]*abstract.TableSchema),
		schemaLock:  sync.Mutex{},
	}
}

func (l *SnapshotLoader) LoadSnapshot(ctx context.Context) error {
	tables, err := ObtainAllSrcTables(l.transfer, l.registry)
	if err != nil {
		return errors.CategorizedErrorf(categories.Source, TableListErrorText, err)
	}
	tableDescriptions := tables.ConvertToTableDescriptions()
	l.schemaLock.Lock()
	for tID, tInfo := range tables {
		l.schemaCache[tID] = tInfo.Schema
	}
	l.schemaLock.Unlock()

	logger.Log.Infof("storage resolved: %d tables in total", len(tables))
	if err := l.CheckIncludeDirectives(tableDescriptions); err != nil {
		return xerrors.Errorf("failed in accordance with configuration: %w", err)
	}
	if err = l.UploadTables(ctx, tableDescriptions, true); err != nil {
		return xerrors.Errorf("failed to upload tables: %w", err)
	}
	return nil
}

func (l *SnapshotLoader) CheckIncludeDirectives(tables []abstract.TableDescription) error {
	unfulfilledIncludes := set.New[string]()
	if l.transfer.DataObjects != nil {
		for _, includeObject := range l.transfer.DataObjects.IncludeObjects {
			requiredTableID, err := abstract.ParseTableID(includeObject)
			if err != nil {
				return xerrors.Errorf("unable to parse table id: %w", err)
			}
			fulfilled := false
			for _, table := range tables {
				if requiredTableID.Includes(table.ID()) {
					fulfilled = true
					break
				}
			}
			if !fulfilled {
				unfulfilledIncludes.Add(includeObject)
			}
		}
	} else if includeable, ok := l.transfer.Src.(model.Includeable); ok {
		unfulfilledIncludes.Add(includeable.AllIncludes()...)
		for _, table := range tables {
			if unfulfilledIncludes.Empty() {
				break
			}
			fulfilledIncludes := includeable.FulfilledIncludes(table.ID())
			unfulfilledIncludes.Remove(fulfilledIncludes...)
		}
	}

	if !unfulfilledIncludes.Empty() {
		return errors.CategorizedErrorf(categories.Source, "some tables from include list are missing in the source database: %v", unfulfilledIncludes.SortedSliceFunc(func(a, b string) bool { return a < b }))
	}
	return nil
}

// TODO Remove, legacy hacks
func (l *SnapshotLoader) endpointsPreSnapshotActions(sourceStorage abstract.Storage) {
	switch specificStorage := sourceStorage.(type) {
	case *greenplum.Storage:
		specificStorage.SetWorkersCount(l.parallelismParams.JobCount)
	}

	if dst, ok := l.transfer.Dst.(model.HackableTarget); ok {
		dst.PreSnapshotHacks()
	}
}

// TODO Remove, legacy hacks
func (l *SnapshotLoader) endpointsPostSnapshotActions() {
	switch dst := l.transfer.Dst.(type) {
	case model.HackableTarget:
		defer dst.PostSnapshotHacks()
	}
}

func (l *SnapshotLoader) applyTransferTmpPolicy(tables []abstract.TableDescription) error {
	if l.transfer.TmpPolicy != nil && l.transfer.TmpPolicy.Suffix != "" {
		if err := model.EnsureTmpPolicySupported(l.transfer.Dst, l.transfer); err != nil {
			return errors.CategorizedErrorf(categories.Target, model.ErrInvalidTmpPolicy, err)
		}
		include := make(map[abstract.TableID]struct{})
		for _, table := range tables {
			tableID := table.ID()
			if table.Filter == "" && table.Offset == 0 {
				include[tableID] = struct{}{}
			} else {
				logger.Log.Infof("table %v excluded from tmp policy due to filter or offset", tableID.Fqtn())
			}
		}
		l.transfer.TmpPolicy = l.transfer.TmpPolicy.WithInclude(func(tableID abstract.TableID) bool {
			_, ok := include[tableID]
			return ok
		})
	}
	return nil
}

func (l *SnapshotLoader) prepareIncrementalState(
	ctx context.Context,
	sourceStorage abstract.Storage,
	tables []abstract.TableDescription,
	updateIncrementalState bool,
) ([]abstract.TableDescription, []abstract.IncrementalState, error) {
	currTables := slices.Clone(tables)
	var err error
	if incrementalStorage, ok := sourceStorage.(abstract.IncrementalStorage); ok {
		currTables, err = l.getIncrementalStateAndMergeWithTables(currTables, incrementalStorage)
		if err != nil {
			return nil, nil, xerrors.Errorf("unable to fill table state: %w", err)
		}
	}

	logger.Log.Info("Preparing incremental state..")
	var nextIncrement []abstract.IncrementalState
	if updateIncrementalState {
		nextIncrement, err = l.getNextIncrementalState(ctx, sourceStorage)
		logger.Log.Infof("Next incremental state: %v", nextIncrement)
		if err != nil {
			return nil, nil, xerrors.Errorf("unable to get next incremental state: %w", err)
		}
		currTables, err = l.mergeIncrementWithTables(currTables, nextIncrement)
		logger.Log.Infof("Merged incremental state: %v", currTables)
		if err != nil {
			return nil, nil, xerrors.Errorf("unable to merge current with next incremental state: %w", err)
		}
	}
	return currTables, nextIncrement, err
}

func (l *SnapshotLoader) updateIncrementalState(updateIncrementalState bool, nextState []abstract.IncrementalState) error {
	if !updateIncrementalState {
		return nil
	}

	err := l.setIncrementalState(nextState)
	if err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to set incremental state: %w", err)
	}
	logger.Log.Info("next incremental state uploaded", log.Any("state", nextState))
	return nil
}

func (l *SnapshotLoader) beginSnapshot(
	ctx context.Context,
	sourceStorage abstract.Storage,
	tables []abstract.TableDescription,
) error {
	switch specificStorage := sourceStorage.(type) {
	case abstract.SnapshotableStorage:
		err := specificStorage.BeginSnapshot(ctx)
		if err != nil {
			return errors.CategorizedErrorf(categories.Source, "Can't begin %s snapshot: %w", l.transfer.SrcType(), err)
		}
	case *postgres.Storage:
		err := specificStorage.BeginPGSnapshot(ctx)
		if err != nil {
			// TODO: change to fatal?
			logger.Log.Warn("unable to begin snapshot", log.Error(err))
		} else {
			logger.Log.Infof("begin postgres snapshot on lsn: %v", specificStorage.SnapshotLSN())
		}
		if !l.transfer.SnapshotOnly() {
			var err error
			tracker := postgres.NewTracker(l.transfer.ID, l.cp)
			l.slotKiller, l.slotKillerErrorChannel, err = specificStorage.RunSlotMonitor(ctx, l.transfer.Src, l.registry, tracker)
			if err != nil {
				return errors.CategorizedErrorf(categories.Source, "failed to start slot monitor: %w", err)
			}
		}
	case *greenplum.Storage:
		if err := specificStorage.BeginGPSnapshot(ctx, tables); err != nil {
			return errors.CategorizedErrorf(categories.Source, "failed to initialize a Greenplum snapshot: %w", err)
		}
		if !l.transfer.SnapshotOnly() {
			var err error
			l.slotKiller, l.slotKillerErrorChannel, err = specificStorage.RunSlotMonitor(ctx, l.transfer.Src, l.registry)
			if err != nil {
				return errors.CategorizedErrorf(categories.Source, "failed to start liveness monitor for Greenplum storage: %w", err)
			}
		}
		workersGpConfig := specificStorage.WorkersGpConfig()
		logger.Log.Info(
			"Greenplum snapshot source runtime configuration",
			log.Any("cluster", workersGpConfig.GetCluster()),
			log.Array("sharding", workersGpConfig.GetWtsList()),
		)
	}
	return nil
}

func (l *SnapshotLoader) endSnapshot(
	ctx context.Context,
	sourceStorage abstract.Storage,
) error {
	switch specificStorage := sourceStorage.(type) {
	case abstract.SnapshotableStorage:
		if err := specificStorage.EndSnapshot(ctx); err != nil {
			logger.Log.Error("Failed to end snapshot", log.Error(err))
		}
	case *postgres.Storage:
		if err := specificStorage.EndPGSnapshot(ctx); err != nil {
			logger.Log.Error("Failed to end snapshot in PostgreSQL", log.Error(err))
		}
	case *greenplum.Storage:
		esCtx, esCancel := context.WithTimeout(context.Background(), greenplum.PingTimeout)
		defer esCancel()
		if err := specificStorage.EndGPSnapshot(esCtx); err != nil {
			logger.Log.Error("Failed to end snapshot in Greenplum", log.Error(err))
			// When we are here, snapshot could not be finished on coordinator.
			// This may be due to various reasons, which include transaction failure (e.g. due to coordinator-standby fallback).
			// For this reason, we must retry the transfer, as the data obtained from Greenplum segments may be inconsistent.
			return errors.CategorizedErrorf(categories.Source, "failed to end snapshot in Greenplum (on coordinator): %w", err)
		}
	}
	return nil
}

func (l *SnapshotLoader) endDestination() error {
	cfg := middlewares.MakeConfig(middlewares.WithNoData)
	baseSink, err := sink.ConstructBaseSink(l.transfer, logger.Log, l.registry, l.cp, cfg)
	if err != nil {
		return xerrors.Errorf("unable to create sink to complete snapshot: %w", err)
	}
	defer func() {
		if err := baseSink.Close(); err != nil {
			logger.Log.Warn("failed sink Close", log.Error(err))
		}
	}()

	completableSink, ok := baseSink.(abstract.Committable)
	if !ok {
		return nil
	}

	return completableSink.Commit()
}

func (l *SnapshotLoader) UploadTables(ctx context.Context, tables []abstract.TableDescription, updateIncrementalState bool) error {
	if updateIncrementalState {
		logger.Log.Info("Checking if we need to update incremental state for this transfer, applicable only for SnapshotOnly type")
		updateIncrementalState = l.transfer.SnapshotOnly() && l.transfer.RegularSnapshot != nil
		logger.Log.Infof("Need to update incremental state: %v, transfer type is SnapshotOnly: %v",
			updateIncrementalState, l.transfer.SnapshotOnly())
	}
	if l.transfer.IsAbstract2() {
		if l.transfer.IsIncremental() && !updateIncrementalState {
			return xerrors.Errorf(
				"upload is not supported for incremental abstract2 transfers (src is %s, dst id %s)",
				l.transfer.Src.GetProviderType().Name(),
				l.transfer.Dst.GetProviderType().Name())
		}

		if err := l.UploadV2(ctx, nil, tables); err != nil {
			return xerrors.Errorf("unable to upload(v2) tables: %w", err)
		}
		return nil
	}

	paralleledRuntime, ok := l.transfer.Runtime.(abstract.ShardingTaskRuntime)

	if !ok || paralleledRuntime.SnapshotWorkersNum() <= 1 {
		if err := l.uploadSingle(ctx, tables, updateIncrementalState); err != nil {
			return xerrors.Errorf("unable to upload tables: %w", err)
		}
		return nil
	}

	if err := l.uploadSharded(ctx, tables, updateIncrementalState); err != nil {
		return xerrors.Errorf("unable to sharded upload tables: %w", err)
	}
	return nil
}

func (l *SnapshotLoader) uploadSharded(ctx context.Context, tables []abstract.TableDescription, updateIncrementalState bool) error {
	if l.transfer.IsMain() {
		if err := l.uploadMain(ctx, tables, updateIncrementalState); err != nil {
			return xerrors.Errorf("unable to sharded upload(main worker) tables: %w", err)
		}
		return nil
	}

	if err := l.uploadSecondary(ctx); err != nil {
		return xerrors.Errorf("unable to sharded upload(secondary worker) tables: %w", err)
	}
	return nil
}

func (l *SnapshotLoader) dumpTablePartsToLogs(parts []*model.OperationTablePart) {
	chunkSize := 1000
	for i := 0; i < len(parts); i += chunkSize {
		end := i + chunkSize
		if end > len(parts) {
			end = len(parts)
		}

		partsToDump := yslices.Map(parts[i:end], func(part *model.OperationTablePart) string {
			return part.String()
		})
		logger.Log.Info(fmt.Sprintf("Tables parts (shards) to copy [%v, %v]", i+1, end), log.Strings("parts", partsToDump))
	}
}

// NewServicePusher returns pusher for sink that provides sinker functionality for `UploadTables()` itself,
// but without middlewares. If no error returned by NewServicePusher you should defer Rollbacks.Do() to close
// created sink.
func (l *SnapshotLoader) NewServicePusher() (abstract.Pusher, *util.Rollbacks, error) {
	cfg := middlewares.MakeConfig(middlewares.WithNoData)
	serviceSink, err := sink.MakeAsyncSink(l.transfer, logger.Log, l.registry, l.cp, cfg)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to create sink: %w", err)
	}

	closeSink := &util.Rollbacks{}
	closeSink.Add(func() {
		if err := serviceSink.Close(); err != nil {
			logger.Log.Warn("service sink's Close failed", log.Error(err))
		}
	})
	return abstract.PusherFromAsyncSink(serviceSink), closeSink, nil
}

func (l *SnapshotLoader) uploadMain(ctx context.Context, inTables []abstract.TableDescription, updateIncrementalState bool) error {
	runtime, ok := l.transfer.Runtime.(abstract.ShardingTaskRuntime)
	if !ok || runtime.SnapshotWorkersNum() <= 1 {
		return errors.CategorizedErrorf(categories.Internal, "run sharding upload with non sharding runtime for operation '%v'", l.operationID)
	}

	if len(inTables) == 0 {
		return abstract.NewFatalError(xerrors.New("no tables in snapshot"))
	}

	if l.transfer.TmpPolicy != nil {
		return abstract.NewFatalError(
			xerrors.Errorf("sharded transfer do not support temporary tables policy, please, turn it off or make transfer not sharded"))
	}

	ctx, l.cancelUpload = context.WithCancel(ctx)
	defer l.cancelUpload()

	sourceStorage, err := storage.NewStorage(l.transfer, l.cp, l.registry)
	if err != nil {
		return errors.CategorizedErrorf(categories.Source, ResolveStorageErrorText, err)
	}
	defer sourceStorage.Close()

	l.endpointsPreSnapshotActions(sourceStorage)

	tables, nextIncrementalState, err := l.startSnapshotIncremental(ctx, inTables, updateIncrementalState, sourceStorage)
	if err != nil {
		return err
	}

	asyncPartsStorage, isAsyncParts := sourceStorage.(abstract.AsyncOperationPartsStorage)

	var parts []*model.OperationTablePart
	if !isAsyncParts {
		parts, err = l.SplitTables(ctx, logger.Log, tables, sourceStorage)
		if err != nil {
			return errors.CategorizedErrorf(categories.Source, "unable to shard tables for operation '%v': %w", l.operationID, err)
		}
		l.dumpTablePartsToLogs(parts)

		if err := l.cp.CreateOperationTablesParts(l.operationID, parts); err != nil {
			return errors.CategorizedErrorf(categories.Internal, "unable to store operation tables: %w", err)
		}
	}

	shardedState, err := l.MainWorkerCreateShardedStateFromSource(sourceStorage)
	if err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to prepare sharded state for operation '%v': %w", l.operationID, err)
	}
	if isAsyncParts {
		newState, err := addKeyToJson(shardedState, abstract.IsAsyncPartsUploadedStateKey, false)
		if err != nil {
			return errors.CategorizedErrorf(categories.Internal, "unable to add key to state: %w", err)
		}
		shardedState = string(newState)
	}

	if err := l.SetShardedStateToCP(logger.Log, shardedState); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to set sharded state: %w", err)
	}

	metricsTracker := NewShardedSnapshotTableMetricsTracker(ctx, l.transfer, l.registry, l.operationID, l.cp)

	if err := l.sendTableControlEvent(ctx, sourceStorage, abstract.InitShardedTableLoad, parts...); err != nil {
		return errors.CategorizedErrorf(categories.Target, "unable to start loading tables: %w", err)
	}

	// Start load tables on secondary workers
	if err := l.cp.CreateOperationWorkers(l.operationID, runtime.SnapshotWorkersNum()); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to create operation workers for operation '%v': %w", l.operationID, err)
	}

	l.waitErrCh = make(chan error, 1)
	go func() {
		defer close(l.waitErrCh)
		logger.Log.Info("Start uploading tables on many workers", log.Int("parallelism", l.parallelismParams.ProcessCount))
		l.waitErrCh <- l.WaitWorkersCompleted(ctx, runtime.SnapshotWorkersNum())
		logger.Log.Info("Uploading tables process on many workers finished")
	}()

	if isAsyncParts {
		if err := l.asyncLoadParts(ctx, asyncPartsStorage, tables, nil); err != nil {
			return errors.CategorizedErrorf(categories.Internal, "unable to async load parts: %w", err)
		}
		newState, err := addKeyToJson(shardedState, abstract.IsAsyncPartsUploadedStateKey, true)
		if err != nil {
			return errors.CategorizedErrorf(categories.Internal, "unable to add key to state: %w", err)
		}
		if err := l.SetShardedStateToCP(logger.Log, string(newState)); err != nil {
			return errors.CategorizedErrorf(categories.Internal, "unable to set sharded state after upload: %w", err)
		}
	}

	if err := l.waitLoaderError(); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "failed to upload %d tables: %w", len(tables), err)
	}

	if err := l.endSnapshot(ctx, sourceStorage); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to end snapshot: %w", err)
	}

	if err := l.sendTableControlEvent(ctx, sourceStorage, abstract.DoneShardedTableLoad, parts...); err != nil {
		return errors.CategorizedErrorf(categories.Target, "unable to finish tables loading: %w", err)
	}

	if err := l.endDestination(); err != nil {
		return errors.CategorizedErrorf(categories.Target, "unable to end snapshot on sink: %v", err)
	}

	if err := l.updateIncrementalState(updateIncrementalState, nextIncrementalState); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to update incremental state: %w", err)
	}

	l.endpointsPostSnapshotActions()

	metricsTracker.Close()

	return nil
}

func (l *SnapshotLoader) startSnapshotIncremental(
	ctx context.Context,
	inTables []abstract.TableDescription,
	updateIncrementalState bool,
	sourceStorage abstract.Storage,
) ([]abstract.TableDescription, []abstract.IncrementalState, error) {
	tables, nextIncrementalState, err := l.prepareIncrementalState(ctx, sourceStorage, inTables, updateIncrementalState)
	if err != nil {
		return nil, nil, errors.CategorizedErrorf(categories.Internal, "unable to prepare incremental state: %w", err)
	}
	logger.Log.Infof("Incremental state for load_snapshot: %v", tables)
	// When using regular incremental snapshot (aka dolivochki) by some cursor field, we assume that this field value
	// only grows monotonically, ie data with id=5 is always accessible when id=6 appears.
	// But strictly speaking, it is never guaranteed, even for ids generated by serial sequence.
	// In postgres it is a common problem, see NOTES section here: https://www.postgresql.org/docs/12/sql-createsequence.html
	// Аs for other data types and DBs, the sequential values may appear in mixed order just because the transaction race:
	// For example:
	//        if transaction A (inserting value '12:00:01') takes just some milliseconds longer,
	//      than transaction B (inserting value '12:00:02'), record with '12:00:02' may become visible when '12:00:01' is not visible yet.
	//      If this B-value happens to be used as next cursor state,
	//      the A-value will not be read neither now - as it is not visible yet, nor next time,  as cursor will be moved forward by that time
	//
	// The simplest way to overcome this situation is to make a pause after getting max_cursor_value and before loading data itself
	// Thus we'll make sure that all concurrent transactions carrying values preceding to max_cursor_value are completed
	// and all data within current cursor borders is visible
	if !updateIncrementalState || l.transfer.RegularSnapshot == nil || l.transfer.RegularSnapshot.IncrementDelaySeconds == 0 {
		logger.Log.Info("No load delay is configured for transfer, starting snapshot immediately")
	} else {
		logger.Log.Infof("Load delay for concurrent transactions is set to '%v', waiting for data to be ready..",
			l.transfer.RegularSnapshot.IncrementDelaySeconds)
		time.Sleep(time.Duration(l.transfer.RegularSnapshot.IncrementDelaySeconds * int64(time.Second)))
	}

	// Note: it is critical to take snapshot only AFTER incremental state was calculated
	// Otherwise, incremental state - which is still taken outside the snapshot - is being calculated on some newer data LSN,
	// so cursor's max value goes beyond data, visible inside snapshot. This leads to potential data LOSS.
	//
	// Timeline of potential problem before this fix:
	// - [X:00:00]: making snapshot on LSN=Y, current max(id) = Z
	// - [X:00:01]: getting incremental state (max(id)), but as time passed, current LSN = Y+1, max(id)=Z+1
	// - [X:00:02]: loading data within snapshot on LSN=Y, using predicate 'where id > last_max_id and id <= Z+1', but id=Z+1 is never present
	// - [X:00:03]: saving new cursor value last_max_id=Z+1
	// - next time will start loading data AFTER Z+1, using predicate 'where id > Z+1 and id <= new_max_id'
	// Record with id=Z+1 will be lost forever
	logger.Log.Info("Will begin snapshot now")
	if err := l.beginSnapshot(ctx, sourceStorage, tables); err != nil {
		return nil, nil, errors.CategorizedErrorf(categories.Internal, "unable to begin snapshot: %w", err)
	}
	return tables, nextIncrementalState, nil
}

func (l *SnapshotLoader) uploadSecondary(ctx context.Context) error {
	runtime, ok := l.transfer.Runtime.(abstract.ShardingTaskRuntime)
	if !ok || runtime.SnapshotWorkersNum() <= 1 {
		return errors.CategorizedErrorf(categories.Internal, "run sharding upload with non sharding runtime for operation '%v'", l.operationID)
	}

	if l.transfer.TmpPolicy != nil {
		return abstract.NewFatalError(
			xerrors.Errorf("sharded transfer do not support temporary tables policy, please, turn it off or make transfer not sharded"))
	}

	ctx, l.cancelUpload = context.WithCancel(ctx)
	defer l.cancelUpload()

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

	if err := AddExtraTransformers(ctx, l.transfer, l.registry); err != nil {
		return xerrors.Errorf("failed to set extra runtime transformations: %w", err)
	}

	sourceStorage, err := storage.NewStorage(l.transfer, l.cp, l.registry)
	if err != nil {
		return errors.CategorizedErrorf(categories.Source, ResolveStorageErrorText, err)
	}
	defer sourceStorage.Close()

	_, isAsyncParts := sourceStorage.(abstract.AsyncOperationPartsStorage)

	l.endpointsPreSnapshotActions(sourceStorage)

	if err := l.SetShardedStateToSource(sourceStorage, shardedState); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "can't set sharded state to storage: %w", err)
	}

	logger.Log.Infof("Start uploading tables on worker %v", l.workerIndex)

	var partProvider TablePartProvider
	if isAsyncParts {
		partProvider = NewAsyncRemoteTablePartProvider(ctx, l.cp, l.getShardStateNoWait, l.operationID, l.transfer.ID, l.workerIndex)
	} else {
		partProvider = NewRemoteTablePartProvider(ctx, l.cp, l.operationID, l.workerIndex)
	}

	if err := l.DoUploadTables(ctx, sourceStorage, partProvider); err != nil {
		return xerrors.Errorf("upload of tables failed on worker '%v': %w", l.workerIndex, err)
	}

	logger.Log.Infof("Done uploading tables on worker %v", l.workerIndex)

	l.endpointsPostSnapshotActions()

	return nil
}

func (l *SnapshotLoader) handleSlotKillerError(err error) error {
	l.cancelUpload()
	logger.Log.Info("slot monitor detected an error", log.Error(err))
	if slotErr := l.slotKiller.KillSlot(); slotErr != nil {
		logger.Log.Warn("failed to kill slot", log.Error(slotErr))
	}
	// the context passed to DoUploadTables has been cancelled, so it is reasonable to wait for the routines to finish
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if uploadErrs := extractErrorsUntil(ctx, l.waitErrCh); uploadErrs != nil {
		logger.Log.Warn("errors during upload", log.Error(uploadErrs))
	}
	return errors.CategorizedErrorf(categories.Source, "slot monitor detected an error: %w", err)
}

func (l *SnapshotLoader) waitLoaderError() error {
	var err error
	select {
	case err = <-l.waitErrCh:
	case err = <-l.slotKillerErrorChannel:
		if err != nil {
			err = xerrors.Errorf("slot killer error: %w", l.handleSlotKillerError(err))
		}
	}
	return err
}

func (l *SnapshotLoader) checkLoaderError() error {
	var err error
	select {
	case err = <-l.waitErrCh:
	case err = <-l.slotKillerErrorChannel:
		if err != nil {
			err = xerrors.Errorf("slot killer error: %w", l.handleSlotKillerError(err))
		}
	default:
	}
	return err
}

func (l *SnapshotLoader) uploadSingle(ctx context.Context, tables []abstract.TableDescription, updateIncrementalState bool) error {
	if len(tables) == 0 {
		return abstract.NewFatalError(xerrors.New("no tables in snapshot"))
	}

	ctx, l.cancelUpload = context.WithCancel(ctx)
	defer l.cancelUpload()

	sourceStorage, err := storage.NewStorage(l.transfer, l.cp, l.registry)
	if err != nil {
		return errors.CategorizedErrorf(categories.Source, ResolveStorageErrorText, err)
	}
	defer sourceStorage.Close()

	if err := AddExtraTransformers(ctx, l.transfer, l.registry); err != nil {
		return xerrors.Errorf("failed to set extra runtime transformations: %w", err)
	}

	if err := l.applyTransferTmpPolicy(tables); err != nil {
		return xerrors.Errorf("failed apply transfer tmp policy: %w", err)
	}

	l.endpointsPreSnapshotActions(sourceStorage)

	tables, nextIncrementalState, err := l.startSnapshotIncremental(ctx, tables, updateIncrementalState, sourceStorage)
	if err != nil {
		return err
	}

	asyncPartsStorage, isAsyncParts := sourceStorage.(abstract.AsyncOperationPartsStorage)

	var partProvider *localTablePartProvider
	var parts []*model.OperationTablePart

	if isAsyncParts {
		logger.Log.Warn("Experimental async parts loading is used")
		partProvider = NewAsyncLocalTablePartProvider()
	} else {
		parts, err = l.SplitTables(ctx, logger.Log, tables, sourceStorage)
		if err != nil {
			return errors.CategorizedErrorf(categories.Source, "unable to shard tables for operation '%v': %w", l.operationID, err)
		}
		for _, table := range parts {
			table.WorkerIndex = new(int)
			*table.WorkerIndex = l.workerIndex // Because we have one worker
		}
		l.dumpTablePartsToLogs(parts)

		if err := l.cp.CreateOperationTablesParts(l.operationID, parts); err != nil {
			return errors.CategorizedErrorf(categories.Internal, "unable to store operation tables: %w", err)
		}
		partProvider = NewLocalTablePartProvider(parts...)
	}

	metricsTracker := NewNotShardedSnapshotTableMetricsTracker(ctx, l.transfer, l.registry, parts, &l.progressUpdateMutex)

	if err := l.sendTableControlEvent(ctx, sourceStorage, abstract.InitShardedTableLoad, parts...); err != nil {
		return errors.CategorizedErrorf(categories.Source, "unable to start loading tables: %w", err)
	}

	l.waitErrCh = make(chan error, 1)
	asyncProviderCtx, cancelAsyncPartsLoading := context.WithCancel(ctx)
	go func() {
		defer close(l.waitErrCh)
		defer cancelAsyncPartsLoading() // Cancel parts loading to prevent deadlocks from localPartProvider.
		logger.Log.Info("Start uploading tables on single worker")
		l.waitErrCh <- l.DoUploadTables(ctx, sourceStorage, partProvider.TablePartProvider())
		logger.Log.Info("Uploading tables process on single worker finished")
	}()

	if isAsyncParts {
		if err := l.asyncLoadParts(asyncProviderCtx, asyncPartsStorage, tables, partProvider); err != nil {
			return errors.CategorizedErrorf(categories.Internal, "unable to async load parts: %w", err)
		}
	}

	if err := l.waitLoaderError(); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "upload of %d tables failed: %w", len(tables), err)
	}

	if err := l.endSnapshot(ctx, sourceStorage); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to end snapshot: %w", err)
	}

	if err := l.sendTableControlEvent(ctx, sourceStorage, abstract.DoneShardedTableLoad, parts...); err != nil {
		return errors.CategorizedErrorf(categories.Target, "unable to finish tables loading: %w", err)
	}

	if err := l.endDestination(); err != nil {
		return errors.CategorizedErrorf(categories.Target, "unable to end snapshot on sink: %v", err)
	}

	logger.Log.Infof("Will update next incremental state for transfer: %v", nextIncrementalState)
	if err := l.updateIncrementalState(updateIncrementalState, nextIncrementalState); err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to update incremental state: %w", err)
	}

	l.endpointsPostSnapshotActions()

	metricsTracker.Close()

	return nil
}

// asyncLoadParts loads parts from sourceProvider to coordinator (CP).
// If localProvider is used (not nil), all parts are also Appended to it and its Close() method is deferredly called.
func (l *SnapshotLoader) asyncLoadParts(ctx context.Context, storage abstract.AsyncOperationPartsStorage, tables []abstract.TableDescription, localProvider *localTablePartProvider) error {
	// Remove all transfer state async parts.
	state, err := l.cp.GetTransferState(l.transfer.ID)
	if err != nil {
		return xerrors.Errorf("unable to get initial transfer state: %w", err)
	}
	var keys []string
	for key := range state {
		if strings.HasPrefix(key, asyncPartsStateKeyPrefix) {
			keys = append(keys, key)
		}
	}
	if len(keys) > 0 {
		logger.Log.Info("Removing initial async parts transfer state", log.Strings("keys", keys))
		if err := l.cp.RemoveTransferState(l.transfer.ID, keys); err != nil {
			logger.Log.Error("Unable to remove initial async parts transfer state",
				log.Strings("keys", keys), log.Error(err))
		}
	}

	if localProvider != nil {
		logger.Log.Info("Starting async load parts with local provider")
		defer localProvider.Close()
	} else {
		logger.Log.Info("Starting async load parts with remote provider")
	}

	totalParts, err := storage.TotalPartsCount(ctx, tables)
	if err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to get total parts count: %w", err)
	}
	storageProvider, cancel, err := storage.AsyncPartsProvider(tables)
	if err != nil {
		return errors.CategorizedErrorf(categories.Internal, "unable to create async parts provider: %w", err)
	}
	defer cancel()

	partNumber := uint64(1)
	for {
		tables, err := storageProvider(ctx, asyncPartsDefaultBatchSize)
		if err != nil {
			return errors.CategorizedErrorf(categories.Internal, "unable to async get table desc: %w", err)
		}
		if len(tables) == 0 {
			break
		}

		parts := make([]*model.OperationTablePart, 0, asyncPartsDefaultBatchSize)
		for _, table := range tables {
			part := model.NewOperationTablePartFromDescription(l.operationID, &table)
			part.PartsCount = totalParts
			part.PartIndex = partNumber
			partNumber++
			parts = append(parts, part)
		}
		if parts, err = l.storePartsFiltersInTransferState(parts); err != nil {
			return errors.CategorizedErrorf(categories.Internal, "unable to store filters in transfer state for %d tables: %w", len(tables), err)
		}
		l.dumpTablePartsToLogs(parts)
		if localProvider != nil {
			if err := localProvider.AppendParts(ctx, parts); err != nil {
				return errors.CategorizedErrorf(categories.Internal, "unable to append parts: %w", err)
			}
		}
		if err := l.cp.CreateOperationTablesParts(l.operationID, parts); err != nil {
			return errors.CategorizedErrorf(categories.Internal, "unable to store operation tables: %w", err)
		}
		if err := l.checkLoaderError(); err != nil {
			return errors.CategorizedErrorf(categories.Internal, "upload of %d tables failed (not all parts published): %w", len(tables), err)
		}
	}
	logger.Log.Info("Async load parts finished successfully")
	return nil
}

// storeFiltersInTransferState saves data from part[i].Filter in TransferState and then replaces part[i].Filter with key
// which can be used to obtain original data in the future. Provided parts not modified, but copied in new slice.
func (l *SnapshotLoader) storePartsFiltersInTransferState(parts []*model.OperationTablePart) ([]*model.OperationTablePart, error) {
	res := make([]*model.OperationTablePart, 0, len(parts))
	state := make(map[string]*coordinator.TransferStateData, len(parts))
	for _, part := range parts {
		stateData := new(coordinator.TransferStateData)
		stateData.Generic = part.Filter
		stateKey := fmt.Sprintf("%s-%s-%d", asyncPartsStateKeyPrefix, l.operationID, part.PartIndex)
		state[stateKey] = stateData
		partCopy := part.Copy()
		partCopy.Filter = stateKey
		res = append(res, partCopy)
	}
	// TODO: Should be increased after TM-8898.
	maxPartsInState := 50
	for { // Wait when state will be less that maxPartsInState.
		state, err := l.cp.GetTransferState(l.transfer.ID)
		if err != nil {
			return nil, xerrors.Errorf("unable to check transfer state: %w", err)
		}
		if len(state) < maxPartsInState {
			break
		}
		if err := l.checkLoaderError(); err != nil {
			return nil, xerrors.Errorf("got upload error when waiting for transfer state clearing: %w", err)
		}
		time.Sleep(5 * time.Minute)
	}
	if err := l.cp.SetTransferState(l.transfer.ID, state); err != nil {
		gotErrorAgain := false
		logger.Log.Error(fmt.Sprintf("Unable to set transfer state of %d keys, logging each and retrying per one", len(state)), log.Error(err))
		for k, v := range state {
			if err := l.cp.SetTransferState(l.transfer.ID, map[string]*coordinator.TransferStateData{k: v}); err != nil {
				logger.Log.Errorf("Still error on key '%s', value (%T) '%v'", k, v.GetGeneric(), v.GetGeneric())
				gotErrorAgain = true
			} else {
				logger.Log.Infof("Pushed key '%s', value (%T) '%v'", k, v.GetGeneric(), v.GetGeneric())
			}
		}
		if gotErrorAgain {
			return nil, xerrors.Errorf("unable to set transfer state of %d keys: %w", len(state), err)
		}
	}
	return res, nil
}

func (l *SnapshotLoader) restorePartFilterFromTransferState(part *model.OperationTablePart) (*model.OperationTablePart, error) {
	res := part.Copy()
	state, err := l.cp.GetTransferState(l.transfer.ID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get transfer state: %w", err)
	}
	stateKey := res.Filter
	filter := state[stateKey].GetGeneric()
	if filter == nil {
		return nil, xerrors.Errorf("unable to get filter by key %s from state %v", stateKey, state)
	}
	filterStr, ok := filter.(string)
	if !ok {
		return nil, xerrors.Errorf("filter from state expected to be string, got %T", filter)
	}
	res.Filter = filterStr
	return res, nil
}

func (l *SnapshotLoader) removePartFilterFromTransferState(part *model.OperationTablePart) error {
	return l.cp.RemoveTransferState(l.transfer.ID, []string{part.Filter})
}

// addKeyToJson unmarshals jsonStr to map[string]any, adds key with value and returns marshalled json.
func addKeyToJson(jsonStr, key string, value any) ([]byte, error) {
	dict := make(map[string]any)
	if jsonStr != "" {
		if err := jsonx.Unmarshal([]byte(jsonStr), &dict); err != nil {
			return nil, xerrors.Errorf("unable to unmarshal JSON string: %w", err)
		}
	}
	dict[key] = value
	return json.Marshal(dict)
}

// extractErrorsUntil extracts errors from the passed channel and places them in a single box
// until either the context is cancelled (or finished), or the passed channel is closed.
func extractErrorsUntil(ctx context.Context, ch <-chan error) error {
	result := util.NewErrs()

overCh:
	for {
		select {
		case <-ctx.Done():
			break overCh
		case err, ok := <-ch:
			if !ok {
				break overCh
			}
			util.AppendErr(result, err)
		}
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

func (l *SnapshotLoader) tableSchema(ctx context.Context, table abstract.TableID, storage abstract.Storage) (*abstract.TableSchema, error) {
	l.schemaLock.Lock()
	defer l.schemaLock.Unlock()
	schema, ok := l.schemaCache[table]
	if ok {
		return schema, nil
	}

	schema, err := storage.TableSchema(ctx, table)
	if err != nil {
		return nil, err
	}

	l.schemaCache[table] = schema
	return schema, nil
}

func (l *SnapshotLoader) sendTableControlEvent(
	ctx context.Context,
	sourceStorage abstract.Storage,
	kind abstract.Kind,
	tables ...*model.OperationTablePart,
) error {
	if kind != abstract.InitShardedTableLoad && kind != abstract.DoneShardedTableLoad {
		return xerrors.Errorf("Unsupported event type '%v'", kind)
	}

	pusher, closeSink, err := l.NewServicePusher()
	if err != nil {
		return errors.CategorizedErrorf(categories.Target, "failed to create pusher: %w", err)
	}
	defer closeSink.Do()

	tablesSet := map[string]bool{}
	for _, table := range tables {
		fqtn := table.TableFQTN()
		if tablesSet[fqtn] {
			continue
		}
		tablesSet[fqtn] = true

		schema, err := l.tableSchema(ctx, *table.ToTableID(), sourceStorage)
		if err != nil {
			return xerrors.Errorf("unable to get schema for table %s: %w", fqtn, err)
		}

		err = pusher([]abstract.ChangeItem{
			{
				Kind:        kind,
				Schema:      table.Schema,
				Table:       table.Name,
				TableSchema: schema,
			},
		})
		if err != nil {
			return xerrors.Errorf("unable to push '%v' for table '%v': %w", kind, fqtn, err)
		}

		logger.Log.Info(
			fmt.Sprintf("Sent control event '%v' for table '%v' on worker %v", kind, fqtn, l.workerIndex),
			log.String("kind", string(kind)), log.String("table", fqtn), log.Int("worker_index", l.workerIndex))
	}
	return nil
}

func (l *SnapshotLoader) sendTablePartControlEvent(event []abstract.ChangeItem, pusher abstract.Pusher, part *model.OperationTablePart) error {
	if len(event) != 1 {
		return xerrors.Errorf("Logic error, wrong control events count, must be 1, but get %v", len(event))
	}

	kind := event[0].Kind

	if err := pusher(event); err != nil {
		return xerrors.Errorf("unable to sent '%v' for table '%v': %w", kind, part, err)
	}

	logger.Log.Info(
		fmt.Sprintf("Sent control event '%v' for table '%v' on worker %v", kind, part, l.workerIndex),
		log.String("kind", string(kind)), log.Any("table_part", part), log.Int("worker_index", l.workerIndex))

	return nil
}

func (l *SnapshotLoader) DoUploadTables(ctx context.Context, source abstract.Storage, nextTablePartProvider TablePartProvider) error {
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

		nextPart, err := nextTablePartProvider(ctx)
		if err != nil {
			parallelismSemaphore.Release(1)
			logger.Log.Error("Unable to get next table to upload", log.Int("worker_index", l.workerIndex), log.Error(ctx.Err()))
			return errors.CategorizedErrorf(categories.Internal, "unable to get next table to upload: %w", err)
		}
		if nextPart == nil {
			parallelismSemaphore.Release(1)
			break // No more tables to transfer
		}

		waitToComplete.Add(1)
		logger.Log.Info(
			fmt.Sprintf("Assigned table part '%v' to worker %v", nextPart, l.workerIndex),
			log.Any("table_part", nextPart), log.Int("worker_index", l.workerIndex))

		go func() {
			defer waitToComplete.Done()
			defer parallelismSemaphore.Release(1)

			upload := func() error {
				if ctx.Err() != nil {
					logger.Log.Warn(
						fmt.Sprintf("Context is canceled while upload table '%v'", nextPart),
						log.Any("table_part", nextPart), log.Error(ctx.Err()))
					return nil
				}

				logger.Log.Info(
					fmt.Sprintf("Start load table '%v' on worker %v", nextPart, l.workerIndex),
					log.Any("table_part", nextPart), log.Int("worker_index", l.workerIndex))

				l.progressUpdateMutex.Lock()
				nextPart.CompletedRows = 0
				nextPart.Completed = false
				l.progressUpdateMutex.Unlock()

				progressTracker.Add(nextPart)

				progress := NewLoadProgress(l.workerIndex, nextPart, &l.progressUpdateMutex)
				sink, err := sink.MakeAsyncSink(
					l.transfer,
					logger.Log,
					l.registry,
					l.cp,
					middlewares.MakeConfig(middlewares.WithEnableRetries),
					progress.SinkOption(),
				)
				if err != nil {
					logger.Log.Error(
						fmt.Sprintf("Failed to create sink for load table '%v' on worker %v", nextPart, l.workerIndex),
						log.Any("table_part", nextPart), log.Int("worker_index", l.workerIndex), log.Error(err))
					if abstract.IsFatal(err) {
						err = backoff.Permanent(err)
					}
					return errors.CategorizedErrorf(categories.Target, "failed to create sink: %w", err)
				}
				closeSink := func() {
					if err := sink.Close(); err != nil {
						logger.Log.Warn(
							fmt.Sprintf("Failed to close sink after load table '%v' on worker %v", nextPart, l.workerIndex),
							log.Any("table_part", nextPart), log.Int("worker_index", l.workerIndex), log.Error(err))
					}
				}
				defer closeSink()

				state := newAsynchronousSnapshotState(sink)
				pusher := state.SnapshotPusher()
				timestampTz := util.GetTimestampFromContextOrNow(ctx)
				schema, err := l.tableSchema(ctx, *nextPart.ToTableID(), source)
				if err != nil {
					return xerrors.Errorf("unable to load table: %s schema:%w", nextPart.String(), err)
				}

				var logPosition abstract.LogPosition
				if positional, ok := source.(abstract.PositionalStorage); ok {
					pos, err := positional.Position(ctx)
					if err != nil {
						return xerrors.Errorf("unable to read LSN: %w", err)
					}
					logPosition = *pos
				}

				initTableLoad := abstract.MakeInitTableLoad(logPosition, *nextPart.ToTableDescription(), timestampTz, schema)
				if err := l.sendTablePartControlEvent(initTableLoad, pusher, nextPart); err != nil {
					return errors.CategorizedErrorf(categories.Target, "unable to start loading table: %w", err)
				}

				var loadTableInput *abstract.TableDescription
				_, isAsyncParts := source.(abstract.AsyncOperationPartsStorage)
				if isAsyncParts {
					fullPart, err := l.restorePartFilterFromTransferState(nextPart)
					if err != nil {
						return xerrors.Errorf("unable to restore part '%v' from state: %w", nextPart, err)
					}
					loadTableInput = fullPart.ToTableDescription()
				} else {
					loadTableInput = nextPart.ToTableDescription()
				}

				if err := source.LoadTable(ctx, *loadTableInput, pusher); err != nil {
					logger.Log.Error(
						fmt.Sprintf("Failed to load table '%v' on worker %v", nextPart, l.workerIndex),
						log.Any("table_part", nextPart), log.Int("worker_index", l.workerIndex), log.Error(err))
					if abstract.IsFatal(err) {
						err = backoff.Permanent(err)
					}
					return errors.CategorizedErrorf(categories.Source, "failed to load table '%s': %w", nextPart, err)
				}

				if isAsyncParts {
					if err := l.removePartFilterFromTransferState(nextPart); err != nil {
						logger.Log.Error("Unable to remove transfer state", log.String("key", nextPart.Filter), log.Error(err))
					}
				}

				doneTableLoad := abstract.MakeDoneTableLoad(logPosition, *nextPart.ToTableDescription(), timestampTz, schema)
				if err := l.sendTablePartControlEvent(doneTableLoad, pusher, nextPart); err != nil {
					return errors.CategorizedErrorf(categories.Target, "unable to finish table loading: %w", err)
				}

				if err := state.Close(); err != nil {
					logger.Log.Error(
						fmt.Sprintf("Failed to deliver items to destination while loading table '%v' on worker %v", nextPart, l.workerIndex),
						log.Any("table_part", nextPart), log.Int("worker_index", l.workerIndex), log.Error(err))
					if abstract.IsFatal(err) {
						err = backoff.Permanent(err)
					}
					return errors.CategorizedErrorf(categories.Target, "failed to deliver items to destination while loading table '%v': %w", nextPart, err)
				}

				l.progressUpdateMutex.Lock()
				nextPart.Completed = true
				l.progressUpdateMutex.Unlock()
				progressTracker.Flush()

				logger.Log.Info(
					fmt.Sprintf(
						"Finish load table '%v' on worker %v, progress %v / %v (%.2f%%)",
						nextPart, l.workerIndex, nextPart.CompletedRows, nextPart.ETARows, nextPart.CompletedPercent()),
					log.Any("table_part", nextPart), log.Int("worker_index", l.workerIndex))

				return nil
			}

			expBackoff := backoff.NewExponentialBackOff()
			expBackoff.MaxElapsedTime = 0
			notify := func(err error, dur time.Duration) {
				logger.Log.Error(
					fmt.Sprintf("Upload table '%v' on worker %v failed, will retry after %s", nextPart, l.workerIndex, dur),
					log.Any("table_part", nextPart), log.Int("worker_index", l.workerIndex), log.Error(err))
			}
			if err := backoff.RetryNotify(upload, backoff.WithMaxRetries(expBackoff, 3), notify); err != nil {
				errorOnce.Do(func() { tableUploadErr = err })
				cancel()
				logger.Log.Error(
					fmt.Sprintf("Upload table '%v' on worker %v, max retries exceeded", nextPart, l.workerIndex),
					log.Any("table_part", nextPart), log.Int("worker_index", l.workerIndex), log.Error(err))
			}
		}()

	}
	waitToComplete.Wait()

	if tableUploadErr != nil {
		return errors.CategorizedErrorf(categories.Internal, "Upload error: %w", tableUploadErr)
	}

	return nil
}
