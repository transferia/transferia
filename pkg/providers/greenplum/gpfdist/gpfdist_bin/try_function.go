package gpfdistbin

import (
	"context"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

var _ error = (*CancelFailedError)(nil)

type CancelFailedError struct{ error }

func (e CancelFailedError) Unwrap() error { return e.error }

func newCancelFailedError(err error) error {
	if err != nil {
		return CancelFailedError{error: err}
	}
	return nil
}

// tryFunction runs `function` and if context canceled cancels it with `cancel`.
// If canceled - `function` will leak in detached goroutine.
// Returns ctx.Err if function successfully canceled.
// Returns CancelFailedError if `cancel` failed.
func tryFunction(ctx context.Context, function, cancel func() error) error {
	fooResCh := make(chan error, 1)
	go func() {
		defer close(fooResCh)
		startedAt := time.Now()
		fooResCh <- function()
		logger.Log.Debugf("tryFunction: Got function return value after %s", time.Since(startedAt).String())
	}()

	select {
	case err := <-fooResCh:
		return err
	case <-ctx.Done():
		if err := cancel(); err != nil {
			return newCancelFailedError(xerrors.Errorf("unable to cancel function: %w", err))
		}
		return nil
	}
}
