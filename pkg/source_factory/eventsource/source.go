package eventsource

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/abstract2"
	"github.com/transferia/transferia/pkg/targets/legacy"
	"go.ytsaurus.tech/library/go/core/log"
)

type eventSourceSource struct {
	source        abstract2.EventSource
	cleanupPolicy model.CleanupType

	logger log.Logger
}

// NewSource constructs a wrapper over the given abstract2.EventSource with the abstract.Source interface
func NewSource(logger log.Logger, source abstract2.EventSource, cleanupPolicy model.CleanupType) abstract.Source {
	return &eventSourceSource{
		source:        source,
		cleanupPolicy: cleanupPolicy,

		logger: logger,
	}
}

func (s *eventSourceSource) Run(sink abstract.AsyncSink) error {
	if s.source.Running() {
		return xerrors.New("Source is already in running state")
	}

	target := legacy.NewEventTarget(s.logger, sink, s.cleanupPolicy)
	if err := s.source.Start(context.Background(), target); err != nil {
		return err
	}
	return nil
}

func (s *eventSourceSource) Stop() {
	if err := s.source.Stop(); err != nil {
		s.logger.Error("Error on source stop", log.Error(err))
	}
}
