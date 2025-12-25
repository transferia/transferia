package local

import (
	"context"
	"sync"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/base"
	"github.com/transferia/transferia/pkg/data"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/sink"
	"github.com/transferia/transferia/pkg/source"
	"github.com/transferia/transferia/pkg/source/eventsource"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.Transfer = (*LocalWorker)(nil)

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

func (w *LocalWorker) RuntimeStatus() abstract.RuntimeStatus {
	return abstract.Unknown
}

func (w *LocalWorker) Start() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		err := runReplication(w.ctx, w.cp, w.transfer, w.registry, w.logger)

		if !util.IsOpen(w.stopCh) {
			// Stopped intentionally via Stop()
			return
		}

		if err != nil {
			w.logger.Error("Local worker error", log.Error(err), log.Any("worker", w))
		}
	}()
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

func (w *LocalWorker) Detach() {
	if util.IsOpen(w.stopCh) {
		w.logger.Infof("Detach: stop monitoring and keeping alive transfer %s", w.transfer.ID)
		close(w.stopCh)
		w.cancel()
	}
}

func (w *LocalWorker) Stop() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.logger.Info("LocalWorker is stopping", log.Any("callstack", util.GetCurrentGoroutineCallstack()))
	w.Detach()

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
	if !util.IsOpen(w.stopCh) {
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
