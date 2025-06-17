//go:build !disable_greenplum_provider

package gpfdistbin

import (
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

// tryFunction runs `function` and `cancel` it if timeout exceeds.
// If timeout reached - `function` will leak in detached goroutine.
// CancelFailedError is returned if `cancel` failed.
// TODO: Move to go/pkg/util or invent other solution.
func tryFunction(function, cancel func() error, timeout time.Duration) error {
	fooResCh := make(chan error, 1)
	go func() {
		defer close(fooResCh)
		startedAt := time.Now()
		fooResCh <- function()
		logger.Log.Debugf("tryFunction: Got function return value after %s", time.Since(startedAt).String())
	}()

	timer := time.NewTimer(timeout)
	var fooErr error
	select {
	case fooErr = <-fooResCh:
	case <-timer.C:
		if err := cancel(); err != nil {
			return newCancelFailedError(xerrors.Errorf("unable to cancel function: %w", err))
		}
		return xerrors.Errorf("function successfully cancelled after its run timeout %s exceeded", timeout.String())
	}
	return fooErr
}
