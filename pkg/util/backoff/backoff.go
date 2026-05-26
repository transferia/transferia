package backoffutil

import (
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

// BackoffLogger uses level "warn" by default
func BackoffLogger(logger log.Logger, msg string) func(error, time.Duration) {
	return BackoffLoggerWarn(logger, msg)
}

const backoffLoggerMsg string = "Will sleep %s and then retry %s because of an error."

func BackoffLoggerWarn(logger log.Logger, msg string) func(error, time.Duration) {
	return func(err error, sleep time.Duration) {
		logger.Warn(fmt.Sprintf(backoffLoggerMsg, sleep, msg), log.Error(err))
	}
}

func BackoffLoggerDebug(logger log.Logger, msg string) func(error, time.Duration) {
	return func(err error, sleep time.Duration) {
		logger.Debug(fmt.Sprintf(backoffLoggerMsg, sleep, msg), log.Error(err))
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
			return res, backoff.Permanent(err)
		}
		//nolint:descriptiveerrors
		return res, err
	}

	return backoff.RetryWithData(oWithFatalHandling, b)
}
