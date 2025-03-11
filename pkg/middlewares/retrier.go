package middlewares

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

// Retrier retries Push operations automatically using the hardcoded delay and interval. Retries can be interrupted using the given context.
// Push operations containing non-row items are NOT retried
func Retrier(logger log.Logger, ctx context.Context) func(abstract.Sinker) abstract.Sinker {
	return func(s abstract.Sinker) abstract.Sinker {
		return newRetrier(s, logger, ctx)
	}
}

type retrier struct {
	sink abstract.Sinker

	ctx         context.Context
	MaxInterval time.Duration

	logger log.Logger
}

func newRetrier(s abstract.Sinker, logger log.Logger, ctx context.Context) *retrier {
	return &retrier{
		sink: s,

		ctx:         ctx,
		MaxInterval: time.Minute * 1,

		logger: logger,
	}
}

func (r *retrier) Close() error {
	return r.sink.Close()
}

func (r *retrier) Push(input []abstract.ChangeItem) error {
	if abstract.ContainsNonRowItem(input) {
		return r.sink.Push(input)
	}

	sinkerBackoff := backoff.NewExponentialBackOff()
	sinkerBackoff.MaxElapsedTime = time.Minute * 3
	// retries for pusher.Push([]ChangeItem):
	// 10% of total upload time, but no more than 1 hour
	uploadDuration := time.Since(util.GetTimestampFromContextOrNow(r.ctx))
	if uploadDuration > time.Hour {
		sinkerBackoff.MaxElapsedTime = uploadDuration / 10
		if sinkerBackoff.MaxElapsedTime > time.Hour {
			sinkerBackoff.MaxElapsedTime = time.Hour
		}
	}
	sinkerBackoff.MaxInterval = time.Minute
	if err := backoff.RetryNotify(func() error {
		select {
		case <-r.ctx.Done():
			return xerrors.Errorf("retries canceled: %w", backoff.Permanent(context.Canceled))
		default:
		}
		if err := r.sink.Push(input); err != nil {
			if abstract.IsFatal(err) || abstract.IsTableUploadError(err) {
				return xerrors.Errorf("retries canceled by non-retriable error: %w", backoff.Permanent(err))
			}
			return err
		}
		return nil
	}, sinkerBackoff, util.BackoffLoggerWarn(r.logger, "Push")); err != nil {
		return xerrors.Errorf("failed to push (with retries): %w", err)
	}
	return nil
}
