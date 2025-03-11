package middlewares

import (
	"github.com/transferria/transferria/pkg/base"
)

// Asynchronizer is tasks.asynchronousSnapshotState for abstract2.
// However, as there is no way to tell if a batch contains "non-row" events, push is always asynchronous.
type Asynchronizer interface {
	Close() error
	Push(input base.EventBatch) error
}

type EventTargetWrapper struct {
	target base.EventTarget
	errChs []chan error
}

func NewEventTargetWrapper(target base.EventTarget) *EventTargetWrapper {
	return &EventTargetWrapper{
		target: target,
		errChs: make([]chan error, 0),
	}
}

func (s *EventTargetWrapper) Close() error {
	for _, errCh := range s.errChs {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}

func (s *EventTargetWrapper) Push(input base.EventBatch) error {
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

	s.errChs = append(s.errChs, s.target.AsyncPush(input))
	return nil
}
