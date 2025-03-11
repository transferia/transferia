package eventsource

import (
	"context"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/base"
	"github.com/transferria/transferria/pkg/targets/legacy"
	"go.ytsaurus.tech/library/go/core/log"
)

type eventSourceSource struct {
	source        base.EventSource
	cleanupPolicy model.CleanupType
	tmpPolicy     *model.TmpPolicyConfig

	logger log.Logger
}

// NewSource constructs a wrapper over the given base.EventSource with the abstract.Source interface
func NewSource(logger log.Logger, source base.EventSource, cleanupPolicy model.CleanupType, tmpPolicy *model.TmpPolicyConfig) abstract.Source {
	return &eventSourceSource{
		source:        source,
		cleanupPolicy: cleanupPolicy,
		tmpPolicy:     tmpPolicy,

		logger: logger,
	}
}

func (s *eventSourceSource) Run(sink abstract.AsyncSink) error {
	if s.source.Running() {
		return xerrors.New("Source is already in running state")
	}

	target := legacy.NewEventTarget(s.logger, sink, s.cleanupPolicy, s.tmpPolicy)
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
