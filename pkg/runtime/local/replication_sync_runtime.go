package local

import (
	"context"
	"sync"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/config/env"
	"github.com/transferia/transferia/pkg/runtime/local/replicationstrategy"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.Transfer = (*LocalWorker)(nil)

type LocalWorker struct {
	transfer *model.Transfer
	cp       coordinator.Coordinator

	registry core_metrics.Registry
	logger   log.Logger

	strategy replicationstrategy.Strategy

	wg     sync.WaitGroup
	stopCh chan struct{}
	mutex  sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc

	initialized bool
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

	if err := w.strategy.Stop(); err != nil {
		return xerrors.Errorf("failed to stop replication strategy: %w", err)
	}

	return nil
}

func (w *LocalWorker) Runtime() abstract.Runtime {
	return new(abstract.LocalRuntime)
}

func (w *LocalWorker) Run() error {
	if err := w.initialize(); err != nil {
		return xerrors.Errorf("failed to initialize LocalWorker: %w", err)
	}

	return w.strategy.Run()
}

func (w *LocalWorker) initialize() error {
	if !util.IsOpen(w.stopCh) {
		return xerrors.New("Stopped before initialization completion")
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if err := tasks.AddExtraTransformers(w.ctx, w.transfer, w.registry); err != nil {
		return xerrors.Errorf("failed to set extra runtime transformations: %w", err)
	}

	var attemptMetrics *stats.ReplicationAttemptMetrics
	if env.In(env.EnvironmentInternal) {
		attemptMetrics = stats.NewReplicationAttemptMetrics(w.registry.WithTags(map[string]string{"resource_id": w.transfer.ID}))
	}
	var err error
	if needPartitionedStrategy(w.transfer) {
		if !w.transfer.RuntimeForReplication().IsSupportedPartitionedStrategy() {
			return abstract.NewFatalError(xerrors.New("partitioned replication is not compatible with MultiYTRuntime"))
		}

		w.strategy, err = replicationstrategy.NewPartitionedStrategy(w.transfer, w.cp, w.registry, w.logger, attemptMetrics)
		if err != nil {
			return xerrors.Errorf("failed to initialize partitioned replication strategy: %w", err)
		}
	} else {
		w.strategy, err = replicationstrategy.NewBasicStrategy(w.transfer, w.cp, w.registry, w.logger, attemptMetrics)
		if err != nil {
			return xerrors.Errorf("failed to initialize basic replication strategy: %w", err)
		}
	}

	w.initialized = true

	return nil
}

func needPartitionedStrategy(transfer *model.Transfer) bool {
	return transfer.IsQueueToS3Replication()
}

func NewLocalWorker(cp coordinator.Coordinator, transfer *model.Transfer, registry core_metrics.Registry, lgr log.Logger) *LocalWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &LocalWorker{
		transfer:    transfer,
		cp:          cp,
		registry:    registry,
		logger:      lgr,
		strategy:    nil,
		stopCh:      make(chan struct{}),
		wg:          sync.WaitGroup{},
		mutex:       sync.Mutex{},
		ctx:         ctx,
		cancel:      cancel,
		initialized: false,
	}
}
