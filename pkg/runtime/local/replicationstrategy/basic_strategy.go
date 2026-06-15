package replicationstrategy

import (
	"sync"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/data"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/sink_factory"
	"github.com/transferia/transferia/pkg/source_factory"
	"github.com/transferia/transferia/pkg/source_factory/eventsource"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type BasicStrategy struct {
	transfer *model.Transfer

	sink                abstract.AsyncSink
	legacySource        abstract.Source
	replicationProvider abstract2.ReplicationProvider
	replicationSource   abstract2.EventSource

	wg sync.WaitGroup

	logger log.Logger
}

func (s *BasicStrategy) Run() error {
	s.wg.Add(1)
	defer s.wg.Done()

	if s.legacySource != nil {
		if err := s.legacySource.Run(s.sink); err != nil {
			return errors.CategorizedErrorf(categories.Source, "failed to run (abstract1 source): %w", err)
		}
		return nil
	}

	if err := eventsource.NewSource(s.logger, s.replicationSource, s.transfer.Dst.CleanupMode()).Run(s.sink); err != nil {
		return errors.CategorizedErrorf(categories.Source, "failed to run (abstract2 source): %w", err)
	}

	return nil
}

func (s *BasicStrategy) Stop() error {
	if s.legacySource != nil {
		s.legacySource.Stop()
	} else {
		if s.replicationSource != nil {
			if err := s.replicationSource.Stop(); err != nil {
				s.logger.Error("error stopping replication source", log.Error(err))
			}
		}
		if s.replicationProvider != nil {
			if err := s.replicationProvider.Close(); err != nil {
				s.logger.Error("error closing replication provider", log.Error(err))
			}
		}
	}
	s.wg.Wait()

	if err := s.sink.Close(); err != nil {
		return xerrors.Errorf("failed to close sink: %w", err)
	}

	return nil
}

func NewBasicStrategy(transfer *model.Transfer, cp coordinator.Coordinator, registry metrics.Registry, logger log.Logger) (*BasicStrategy, error) {
	basicStrategy := &BasicStrategy{
		transfer: transfer,

		sink:                nil,
		legacySource:        nil,
		replicationProvider: nil,
		replicationSource:   nil,

		wg: sync.WaitGroup{},

		logger: logger,
	}

	var err error
	basicStrategy.sink, err = sink_factory.MakeAsyncSink(transfer, new(model.TransferOperation), logger, registry, cp, middlewares.MakeConfig(middlewares.AtReplicationStage))
	if err != nil {
		return nil, errors.CategorizedErrorf(categories.Target, "failed to create sink: %w", err)
	}

	rollbacks := util.Rollbacks{}
	defer rollbacks.Do()
	rollbacks.Add(func() {
		if err := basicStrategy.sink.Close(); err != nil {
			logger.Error("failed to close sink", log.Error(err))
		}
	})

	dataProvider, err := data.NewDataProvider(
		logger,
		registry,
		transfer,
		cp,
	)
	if err != nil {
		if xerrors.Is(err, data.TryLegacySourceError) {
			basicStrategy.legacySource, err = source_factory.NewSource(transfer, logger, registry, cp)
		}
		if err != nil {
			return nil, errors.CategorizedErrorf(categories.Source, "failed to create source: %w", err)
		}
	} else {
		if replicationProvider, ok := dataProvider.(abstract2.ReplicationProvider); ok {
			basicStrategy.replicationProvider = replicationProvider
			if err := basicStrategy.replicationProvider.Init(); err != nil {
				return nil, errors.CategorizedErrorf(categories.Source, "failed to initialize replication provider: %w", err)
			}
			basicStrategy.replicationSource, err = replicationProvider.CreateReplicationSource()
			if err != nil {
				return nil, errors.CategorizedErrorf(categories.Source, "failed to create replication source: %w", err)
			}
		} else {
			if err := dataProvider.Close(); err != nil {
				logger.Warn("unable to close data provider", log.Error(err))
			}
			return nil, xerrors.New("invalid data provider type: expected ReplicationProvider")
		}
	}
	rollbacks.Cancel()

	return basicStrategy, nil
}
