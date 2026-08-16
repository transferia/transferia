package backoffutil

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

const backoffLoggerMsg string = "Will sleep %s and then retry %s because of an error."

// Log uses level "warn" by default and preserves context log fields.
func Log(ctx context.Context, msg string) func(error, time.Duration) {
	return LogWarn(ctx, msg)
}

func LogWithLogger(ctx context.Context, logger log.Logger, msg string) func(error, time.Duration) {
	return LogWarnWithLogger(ctx, logger, msg)
}

func LogWarn(ctx context.Context, msg string) func(error, time.Duration) {
	return LogWarnWithLogger(ctx, logger.Log, msg)
}

func LogWarnWithLogger(ctx context.Context, l log.Logger, msg string) func(error, time.Duration) {
	return func(err error, sleep time.Duration) {
		logger.WarnWithLogger(ctx, l, fmt.Sprintf(backoffLoggerMsg, sleep, msg), log.Error(err))
	}
}

func LogDebug(ctx context.Context, msg string) func(error, time.Duration) {
	return LogDebugWithLogger(ctx, logger.Log, msg)
}

func LogDebugWithLogger(ctx context.Context, l log.Logger, msg string) func(error, time.Duration) {
	return func(err error, sleep time.Duration) {
		logger.DebugWithLogger(ctx, l, fmt.Sprintf(backoffLoggerMsg, sleep, msg), log.Error(err))
	}
}

func NewExponentialBackOff(opts ...backoff.ExponentialBackOffOpts) *backoff.ExponentialBackOff {
	currOpts := make([]backoff.ExponentialBackOffOpts, 0, len(opts)+1)
	currOpts = append(currOpts, backoff.WithMaxElapsedTime(0)) // turn-off MaxElapsedTime
	currOpts = append(currOpts, opts...)
	return backoff.NewExponentialBackOff(currOpts...)
}

func RetryWithData[T any](o backoff.OperationWithData[T], b backoff.BackOff) (T, error) {
	oWithFatalHandling := func() (T, error) {
		res, err := o()
		if err != nil && abstract.IsFatal(err) {
			//nolint:descriptiveerrors
			return res, Permanent(err)
		}
		//nolint:descriptiveerrors
		return res, err
	}

	return backoff.RetryWithData(oWithFatalHandling, b)
}

func Permanent(err error) error {
	return backoff.Permanent(err)
}

func RetryNotify(operation backoff.Operation, b backoff.BackOff, notify backoff.Notify) error {
	return backoff.RetryNotify(operation, b, notify)
}

func WithContext(b backoff.BackOff, ctx context.Context) backoff.BackOff {
	return backoff.WithContext(b, ctx)
}

func WithMaxRetries(b backoff.BackOff, maxRetries uint64) backoff.BackOff {
	return backoff.WithMaxRetries(b, maxRetries)
}
