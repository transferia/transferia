package local

import (
	"context"
	"sync"

	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/base"
	"github.com/transferria/transferria/pkg/data"
	"github.com/transferria/transferria/pkg/errors"
	"github.com/transferria/transferria/pkg/errors/categories"
	"github.com/transferria/transferria/pkg/middlewares"
	"github.com/transferria/transferria/pkg/sink"
	"github.com/transferria/transferria/pkg/source"
	"github.com/transferria/transferria/pkg/source/eventsource"
	"github.com/transferria/transferria/pkg/util"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"go.ytsaurus.tech/library/go/core/log"
)

type LocalWorker struct {
	transfer            *model.Transfer
	registry            metrics.Registry
	logger              log.Logger
	sink                abstract.AsyncSink
	legacySource        abstract.Source
	replicationProvider base.ReplicationProvider
	replicationSource   base.EventSource
	wg                  sync.WaitGroup
	stopCh              chan struct{}
	mutex               sync.Mutex
	initialized         bool
	cp                  coordinator.Coordinator
	ctx                 context.Context
	cancel              context.CancelFunc
}

func (w *LocalWorker) Error() error {
	return nil
}

func (w *LocalWorker) Start() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		err := runReplication(w.ctx, w.cp, w.transfer, w.registry, w.logger)

		if !isOpen(w.stopCh) {
			// Stopped intentionally via Stop()
			return
		}

		if err != nil {
			w.logger.Error("Local worker error", log.Error(err), log.Any("worker", w))
		}
	}()
}

func isOpen(ch chan struct{}) bool {
	select {
	case <-ch:
		return false
	default:
		return true
	}
}

func (w *LocalWorker) StopReplicationSource() {
	if w.replicationSource != nil {
		if err := w.replicationSource.Stop(); err != nil {
			w.logger.Error("Error on stop replication source", log.Error(err))
		}
	}
	if w.replicationProvider != nil {
		if err := w.replicationProvider.Close(); err != nil {
			w.logger.Error("Error on close replication provider", log.Error(err))
		}
	}
}

func (w *LocalWorker) Stop() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.logger.Info("LocalWorker is stopping", log.Any("callstack", util.GetCurrentGoroutineCallstack()))

	if isOpen(w.stopCh) {
		close(w.stopCh)
		w.cancel()
	}

	if !w.initialized {
		// Not started yet
		return nil
	}

	if w.legacySource != nil {
		w.legacySource.Stop()
	} else {
		w.StopReplicationSource()
	}
	w.wg.Wait()
	if err := w.sink.Close(); err != nil {
		return xerrors.Errorf("failed to close sink: %w", err)
	}

	return nil
}

func (w *LocalWorker) Runtime() abstract.Runtime {
	return new(abstract.LocalRuntime)
}

func (w *LocalWorker) initialize() (err error) {
	if !isOpen(w.stopCh) {
		return xerrors.New("Stopped before initialization completion")
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	var rollbacks util.Rollbacks
	defer rollbacks.Do()

	if err := tasks.AddExtraTransformers(w.ctx, w.transfer, w.registry); err != nil {
		return xerrors.Errorf("failed to set extra runtime transformations: %w", err)
	}
	w.sink, err = sink.MakeAsyncSink(w.transfer, w.logger, w.registry, w.cp, middlewares.MakeConfig(middlewares.AtReplicationStage))
	if err != nil {
		return errors.CategorizedErrorf(categories.Target, "failed to create sink: %w", err)
	}
	rollbacks.Add(func() {
		if err := w.sink.Close(); err != nil {
			w.logger.Error("Failed to close sink", log.Error(err))
		}
	})

	dataProvider, err := data.NewDataProvider(
		w.logger,
		w.registry,
		w.transfer,
		w.cp,
	)
	if err != nil {
		if xerrors.Is(err, data.TryLegacySourceError) {
			w.legacySource, err = source.NewSource(w.transfer, w.logger, w.registry, w.cp)
		}
		if err != nil {
			return errors.CategorizedErrorf(categories.Source, "failed to create source: %w", err)
		}
	} else {
		if replicationProvider, ok := dataProvider.(base.ReplicationProvider); ok {
			w.replicationProvider = replicationProvider
			if err := w.replicationProvider.Init(); err != nil {
				return errors.CategorizedErrorf(categories.Source, "failed to initialize replication provider: %w", err)
			}
			w.replicationSource, err = replicationProvider.CreateReplicationSource()
			if err != nil {
				return errors.CategorizedErrorf(categories.Source, "failed to create replication source: %w", err)
			}
		} else {
			if err := dataProvider.Close(); err != nil {
				w.logger.Warn("unable to close data provider", log.Error(err))
			}
			return xerrors.New("Data provider must be ReplicationProvider")
		}
	}
	rollbacks.Cancel()
	w.initialized = true
	return nil
}

func (w *LocalWorker) Run() error {
	if err := w.initialize(); err != nil {
		return xerrors.Errorf("failed to initialize LocalWorker: %w", err)
	}

	if w.legacySource != nil {
		if err := w.legacySource.Run(w.sink); err != nil {
			return errors.CategorizedErrorf(categories.Source, "failed to run (abstract1 source): %w", err)
		}
		return nil
	}
	if err := eventsource.NewSource(w.logger, w.replicationSource, w.transfer.Dst.CleanupMode(), w.transfer.TmpPolicy).Run(w.sink); err != nil {
		return errors.CategorizedErrorf(categories.Source, "failed to run (abstract2 source): %w", err)
	}
	return nil
}

func NewLocalWorker(cp coordinator.Coordinator, transfer *model.Transfer, registry metrics.Registry, lgr log.Logger) *LocalWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &LocalWorker{
		transfer:            transfer,
		registry:            registry,
		logger:              lgr,
		stopCh:              make(chan struct{}),
		cp:                  cp,
		sink:                nil,
		legacySource:        nil,
		replicationProvider: nil,
		replicationSource:   nil,
		wg:                  sync.WaitGroup{},
		mutex:               sync.Mutex{},
		initialized:         false,
		ctx:                 ctx,
		cancel:              cancel,
	}
}
