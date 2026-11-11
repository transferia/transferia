package validator

import (
	"context"
	"errors"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

type Aggregator struct {
	sinks []abstract.Sinker
}

var _ abstract.Committable = (*Aggregator)(nil)

func (c *Aggregator) Close() error {
	var errs []error
	for _, sink := range c.sinks {
		if err := sink.Close(); err != nil {
			logger.Log.Error("unable to close", log.Error(err))
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return abstract.NewFatalError(xerrors.Errorf("sink failed to close: %w", err))
	}
	return nil
}

func (c *Aggregator) Push(items []abstract.ChangeItem) error {
	var errs []error
	for _, sink := range c.sinks {
		if err := sink.Push(items); err != nil {
			logger.Log.Error("unable to push", log.Error(err))
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return abstract.NewFatalError(xerrors.Errorf("sink failed to push: %w", err))
	}
	return nil
}

func (c *Aggregator) Commit(ctx context.Context) error {
	var errs []error
	for _, sink := range c.sinks {
		committable, ok := sink.(abstract.Committable)
		if ok {
			if err := committable.Commit(ctx); err != nil {
				logger.Log.Error("unable to commit", log.Error(err))
				errs = append(errs, err)
			}
		}
	}
	if err := errors.Join(errs...); err != nil {
		return abstract.NewFatalError(xerrors.Errorf("sink failed to commit: %w", err))
	}
	return nil
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
