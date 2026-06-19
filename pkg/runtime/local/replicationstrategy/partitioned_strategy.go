package replicationstrategy

import (
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/sink_factory"
	"github.com/transferia/transferia/pkg/source_factory"
	backoffutil "github.com/transferia/transferia/pkg/util/backoff"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

type newSourceF func(partition abstract.Partition) (abstract.QueueToS3Source, error)
type newSinkF func(partition abstract.Partition) (abstract.QueueToS3Sink, error)
type newListerF func() (abstract.PartitionLister, error)

type PartitionedStrategy struct {
	errCh             chan error
	stopCh            chan struct{}
	mutex             sync.Mutex
	partitionToRunner map[abstract.Partition]*partitionRunner

	currWorkerIndex int
	totalWorkersNum int

	newSource newSourceF
	newSink   newSinkF
	newLister newListerF

	lister abstract.PartitionLister

	logger log.Logger
}

func (s *PartitionedStrategy) Run() error {
	if err := s.syncRunnersWithPartitions(); err != nil {
		return xerrors.Errorf("unable to synchronize runners with partitions initially: %w", err)
	}

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.syncRunnersWithPartitions(); err != nil {
				return xerrors.Errorf("unable to synchronize runners with partitions: %w", err)
			}

		case <-s.stopCh:
			if err := s.runnerErrors(); err != nil {
				return xerrors.Errorf("runner execution stopped with errors: %w", err)
			}
			return nil

		case <-s.errCh:
			if err := s.stopRunnersAndLister(); err != nil {
				return xerrors.Errorf("unable to stop runners during shutdown: %w", err)
			}
			if err := s.runnerErrors(); err != nil {
				return xerrors.Errorf("running stopped with errors: %w", err)
			}
			return nil
		}
	}
}

func (s *PartitionedStrategy) Stop() error {
	if err := s.stopRunnersAndLister(); err != nil {
		return err
	}
	close(s.stopCh)

	return nil
}

func (s *PartitionedStrategy) stopRunnersAndLister() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.closeLister()

	var errs []error
	for _, currRunner := range s.partitionToRunner {
		if err := currRunner.stop(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return xerrors.Errorf("failed to stop runners: %w", errors.Join(errs...))
	}

	return nil
}

func (s *PartitionedStrategy) runnerErrors() error {
	var errs []error
	for _, runner := range s.partitionToRunner {
		if runner.err != nil {
			errs = append(errs, runner.err)
		}
	}
	if len(errs) > 0 {
		return xerrors.Errorf("runner errors: %w", errors.Join(errs...))
	}

	return nil
}

func (s *PartitionedStrategy) syncRunnersWithPartitions() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	partitions, err := s.getOrderedPartitions()
	if err != nil {
		return xerrors.Errorf("failed to get ordered partitions: %w", err)
	}
	s.logger.Debug("partitions listed for runner synchronization", log.Any("partitions", partitions))

	currWorkerPartitions := assignedPartitions(s.currWorkerIndex, s.totalWorkersNum, partitions)
	s.logger.Debug("partitions assigned to current worker", log.Int("worker_index", s.currWorkerIndex),
		log.Int("total_workers", s.totalWorkersNum), log.Any("assigned_partitions", currWorkerPartitions))

	// create missing runners
	for _, partition := range currWorkerPartitions {
		if _, ok := s.partitionToRunner[partition]; ok {
			continue
		}

		partitionSource, err := s.newSource(partition)
		if err != nil {
			return xerrors.Errorf("failed to create source: %w", err)
		}

		partitionSink, err := s.newSink(partition)
		if err != nil {
			return xerrors.Errorf("failed to create sink: %w", err)
		}

		currRunner := newPartitionRunner(partitionSource, partitionSink)
		currRunner.run(s.errCh)

		s.partitionToRunner[partition] = currRunner
		s.logger.Debug("runner was created for partition", log.Any("partition", partition))
	}

	// delete extra runners
	partitionSet := set.New(currWorkerPartitions...)
	for partition, runner := range s.partitionToRunner {
		if !partitionSet.Contains(partition) {
			if err := runner.stop(); err != nil {
				return xerrors.Errorf("failed to stop runner for partition %v: %w", partition, err)
			}
			delete(s.partitionToRunner, partition)
			s.logger.Debug("runner was stopped and removed", log.Any("partition", partition))
		}
	}

	return nil
}

func (s *PartitionedStrategy) getOrderedPartitions() ([]abstract.Partition, error) {
	partitions, err := s.listPartitions()
	if err != nil {
		return nil, xerrors.Errorf("failed to get partitions: %w", err)
	}

	slices.SortFunc(partitions, func(a, b abstract.Partition) int {
		if a.Topic == b.Topic {
			return int(a.Partition) - int(b.Partition)
		}

		if a.Topic < b.Topic {
			return -1
		}
		return 1
	})

	return partitions, nil
}

func (s *PartitionedStrategy) listPartitions() ([]abstract.Partition, error) {
	partitions, err := backoffutil.RetryWithData(func() ([]abstract.Partition, error) {
		if err := s.ensureLister(); err != nil {
			return nil, xerrors.Errorf("failed to ensure lister: %w", err)
		}

		partitions, err := s.lister.ListPartitions()
		if err != nil {
			s.closeLister()
			return nil, xerrors.Errorf("failed to list partitions: %w", err)
		}

		return partitions, nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))
	if err != nil {
		return nil, xerrors.Errorf("unable to list partitions with retries: %w", err)
	}

	return partitions, nil
}

func (s *PartitionedStrategy) ensureLister() error {
	if s.lister == nil {
		lister, err := s.newLister()
		if err != nil {
			return err
		}
		s.lister = lister
	}

	return nil
}

func (s *PartitionedStrategy) closeLister() {
	if s.lister != nil {
		s.lister.Close()
		s.lister = nil
	}
}

// assignedPartitions returns a slice of partitions assigned to
// the specified worker using round-robin distribution.
func assignedPartitions(workerIndex, totalWorkers int, allPartitions []abstract.Partition) []abstract.Partition {
	if len(allPartitions) == 0 {
		return []abstract.Partition{}
	}

	result := make([]abstract.Partition, 0)
	for _, partition := range allPartitions {
		if int(partition.Partition)%totalWorkers == workerIndex {
			result = append(result, partition)
		}
	}

	return result
}

func NewPartitionedStrategy(transfer *model.Transfer, cp coordinator.Coordinator, registry metrics.Registry, logger log.Logger) (*PartitionedStrategy, error) {
	shardingRuntime, ok := transfer.RuntimeForReplication().(abstract.ShardingTaskRuntime)
	if !ok {
		return nil, xerrors.New("unexpected runtime type")
	}

	partitionedStrategy := &PartitionedStrategy{
		errCh:             make(chan error, 1),
		stopCh:            make(chan struct{}),
		mutex:             sync.Mutex{},
		partitionToRunner: make(map[abstract.Partition]*partitionRunner),

		currWorkerIndex: shardingRuntime.CurrentJobIndex(),
		totalWorkersNum: shardingRuntime.ReplicationWorkersNum(),

		newSource: nil,
		newSink:   nil,
		newLister: nil,

		lister: nil,

		logger: logger,
	}

	partitionedStrategy.newSource = func(partition abstract.Partition) (abstract.QueueToS3Source, error) {
		return source_factory.NewPartitionableSource(transfer, logger, registry, cp, partition)
	}

	partitionedStrategy.newSink = func(partition abstract.Partition) (abstract.QueueToS3Sink, error) {
		return sink_factory.MakeAsyncReplicationSink(
			transfer,
			new(model.TransferOperation),
			logger,
			registry,
			cp,
			middlewares.MakeConfig(middlewares.AtReplicationStage),
			partition,
		)
	}

	partitionedStrategy.newLister = func() (abstract.PartitionLister, error) {
		return source_factory.NewPartitionLister(transfer, logger, registry, cp)
	}

	return partitionedStrategy, nil
}
