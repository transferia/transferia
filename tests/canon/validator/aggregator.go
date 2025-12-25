package validator

import (
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type Aggregator struct {
	sinks []abstract.Sinker
}

func (c *Aggregator) Close() error {
	var errs util.Errors
	for _, sink := range c.sinks {
		if err := sink.Close(); err != nil {
			logger.Log.Error("unable to close", log.Error(err))
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return abstract.NewFatalError(xerrors.Errorf("sink failed to close: %w", errs))
}

func (c *Aggregator) Push(items []abstract.ChangeItem) error {
	var errs util.Errors
	for _, sink := range c.sinks {
		if err := sink.Push(items); err != nil {
			logger.Log.Error("unable to push", log.Error(err))
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return abstract.NewFatalError(xerrors.Errorf("sink failed to push: %w", errs))
}

func (c *Aggregator) Commit() error {
	var errs util.Errors
	for _, sink := range c.sinks {
		committable, ok := sink.(abstract.Committable)
		if ok {
			if err := committable.Commit(); err != nil {
				logger.Log.Error("unable to commit", log.Error(err))
				errs = append(errs, err)
			}
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return abstract.NewFatalError(xerrors.Errorf("sink failed to commit: %w", errs))
}

func New(isStrictSource bool, factories ...func() abstract.Sinker) func() abstract.Sinker {
	return func() abstract.Sinker {
		var childSinks []abstract.Sinker
		for _, factory := range factories {
			childSinks = append(childSinks, factory())
		}
		if isStrictSource {
			childSinks = append(childSinks, valuesStrictTypeChecker())
		}
		return &Aggregator{
			sinks: childSinks,
		}
	}
}
