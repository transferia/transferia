package tasks

// XXX: This must be replaced with providers/middlewares `Asynchronizer` when abstract1 is dropped.

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// asynchronousSnapshotState provides a wrapper around asynchronous sink with synchronous abstract.Pusher interface which works lazily.
// A push returns an error that happened in one of the previous pushes and nil otherwise.
// The error channel for each asynchronous push is saved in an internal array. These are waited on when the state is closed.
//
// This mechanism is aimed at snapshot transfers, as they do not require intermediate results and can be completely asynchronous up to the finish.
type asynchronousSnapshotState struct {
	sink abstract.AsyncSink

	errChs []chan error
}

func newAsynchronousSnapshotState(sink abstract.AsyncSink) *asynchronousSnapshotState {
	return &asynchronousSnapshotState{
		sink: sink,

		errChs: make([]chan error, 0),
	}
}

// SnapshotPusher returns a pusher for snapshot transfers. It pushes batches completely asynchronously (in a "lazy" manner), except for batches containing non-row items, which are pushed synchronously.
func (s *asynchronousSnapshotState) SnapshotPusher() abstract.Pusher {
	return s.push
}

// Close waits on all cached error channels and returns the first error encountered.
//
// This method does NOT call underlying sink's Close.
func (s *asynchronousSnapshotState) Close() error {
	for _, errCh := range s.errChs {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}

func (s *asynchronousSnapshotState) push(items []abstract.ChangeItem) error {
	var result error = nil
	lastReadChI := 0

overErrChs:
	for _, errCh := range s.errChs {
		select {
		case err := <-errCh:
			lastReadChI += 1
			if err != nil {
				result = err
				break overErrChs
			}
		default:
			break overErrChs
		}
	}
	s.errChs = s.errChs[lastReadChI:]
	if result != nil {
		return result
	}

	// non row items are pushed synchronously, otherwise some subsequent event may be pushed even if
	// some previous event was processed with error, e.g., DoneTableLoad after InitTableLoad
	if abstract.ContainsNonRowItem(items) {
		err := <-s.sink.AsyncPush(items)
		if err != nil {
			return xerrors.Errorf("unable push batch with a non row item: %w", err)
		}
		return nil
	}

	s.errChs = append(s.errChs, s.sink.AsyncPush(items))
	return nil
}
