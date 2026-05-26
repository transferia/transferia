package replicationstrategy

import (
	"sync"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
)

type partitionRunner struct {
	source abstract.QueueToS3Source
	sink   abstract.QueueToS3Sink

	wg sync.WaitGroup

	err       error
	isStopped bool
}

func (r *partitionRunner) run(runningErrCh chan<- error) {
	r.wg.Add(1)

	go func() {
		defer r.wg.Done()

		r.err = r.source.Run(r.sink)
		// We do not send an error to the channel if the runner is stopped,
		// because only running errors should get into the channel.
		if !r.isStopped {
			util.TrySend(runningErrCh, r.err)
		}
	}()
}

func (r *partitionRunner) stop() error {
	r.isStopped = true
	r.source.Stop()
	r.wg.Wait()

	if err := r.sink.Close(); err != nil {
		return err
	}

	return nil
}

func newPartitionRunner(source abstract.QueueToS3Source, sink abstract.QueueToS3Sink) *partitionRunner {
	return &partitionRunner{
		source:    source,
		sink:      sink,
		wg:        sync.WaitGroup{},
		err:       nil,
		isStopped: false,
	}
}
