package parsequeue

// WaitableQueue unifies standard (AsyncSink) and queue-to-S3 (QueueToS3Sink) replication modes,
// allowing sources to work with both WaitableQueue implementations through a common interface.
type WaitableQueue[TData any] interface {
	Add(data TData) error
	Wait()
	Close()

	// Error returns the first error that occurred during queue operation.
	// Error state is guaranteed to be final after Close() completes.
	//
	// Safe to call concurrently and after Close().
	Error() error
	// Done returns a channel that is closed after the parse queue is closed or an error occurs in it.
	Done() <-chan struct{}
}
