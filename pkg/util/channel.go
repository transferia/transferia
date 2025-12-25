package util

import (
	"context"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// IsOpen
// Beware, this function consumes an item from the channel. Use only on channels without
// data, i.e. which are only closed but not written into.
func IsOpen(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return false
	default:
		return true
	}
}

func ParallelDo(ctx context.Context, count, parallelDegree int, executor func(i int) error) error {
	wg := sync.WaitGroup{}
	wg.Add(count)
	sem := semaphore.NewWeighted(int64(parallelDegree))
	errCh := make(chan error, count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			if err := sem.Acquire(ctx, 1); err != nil {
				errCh <- err
				return
			}
			defer sem.Release(1)
			if err := executor(i); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	return WrapErrCh(xerrors.New("Failed to execute"), errCh)
}

func ParallelDoWithContextAbort(ctx context.Context, count, parallelDegree int, executor func(i int, ctx context.Context) error) error {
	eg, subCtx := errgroup.WithContext(ctx)
	if parallelDegree < 1 {
		parallelDegree = 1
	}
	sem := semaphore.NewWeighted(int64(parallelDegree))
	for i := 0; i < count; i++ {
		task := i
		eg.Go(func() error {
			select {
			case <-subCtx.Done():
				return xerrors.NewSentinel("context was canceled")
			default:
			}
			if err := sem.Acquire(subCtx, 1); err != nil {
				return xerrors.Errorf("failed to acquire semaphore : %w", err)
			}
			defer sem.Release(1)
			if err := executor(task, subCtx); err != nil {
				return xerrors.Errorf("failed to execute: %w", err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return xerrors.Errorf("one of the goroutines failed: %w", err)
	}
	return nil
}

func Send[T any](ctx context.Context, workCh chan<- T, val T) bool {
	select {
	case <-ctx.Done():
		return false
	case workCh <- val:
		return true
	}
}

func Receive[T any](ctx context.Context, workCh <-chan T) (T, bool) {
	select {
	case <-ctx.Done():
		var t T
		return t, false
	case t := <-workCh:
		return t, true
	}
}
