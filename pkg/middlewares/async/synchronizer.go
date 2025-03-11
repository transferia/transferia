package async

import (
	"sync"

	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

// Synchronizer provides AsyncPush which is executed synchronously with the underlying synchronous Push.
// In other words, it provides synchronous replication pipeline.
//
// It has the required built-in mutexes, so that it does not call underlying sink concurrently.
func Synchronizer(logger log.Logger) func(abstract.Sinker) abstract.AsyncSink {
	return func(s abstract.Sinker) abstract.AsyncSink {
		return newSynchronizer(s, logger)
	}
}

type synchronizer struct {
	sink abstract.Sinker

	mtx    sync.Mutex
	closed bool

	logger log.Logger
}

func newSynchronizer(sink abstract.Sinker, logger log.Logger) *synchronizer {
	return &synchronizer{
		sink: sink,

		mtx:    sync.Mutex{},
		closed: false,

		logger: logger,
	}
}

func (s *synchronizer) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	return s.sink.Close()
}

func (s *synchronizer) AsyncPush(items []abstract.ChangeItem) chan error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if s.closed {
		return util.MakeChanWithError(abstract.AsyncPushConcurrencyErr)
	}

	result := util.MakeChanWithError(s.sink.Push(items))
	s.logger.Info("Synchronous Push has finished", log.Int("len", len(items)))
	return result
}
