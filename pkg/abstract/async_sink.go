package abstract

import (
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

// AsyncSink provides asynchronous Push operation, which should be a wrapper over synchronous Push implemented by sink.
//
// All of its methods may be called concurrently.
type AsyncSink interface {
	io.Closer
	// AsyncPush writes items asynchronously. The error for the given batch of items will be written into the resulting channel when an underlying (synchronous) Push actually happens.
	// Note, that AsyncPush takes ownership on slice `items`, so it shouldn't be further used.
	AsyncPush(items []ChangeItem) chan error
}

// AsyncPushConcurrencyErr indicates a Push has been called on an already closed AsyncSink. This must not happen and means there are concurrency issues in the implementation of a source.
var AsyncPushConcurrencyErr = xerrors.NewSentinel("AsyncPush is called after Close")

// PusherFromAsyncSink wraps the given sink into a (synchronous) pusher interface
func PusherFromAsyncSink(asink AsyncSink) Pusher {
	return func(items []ChangeItem) error {
		return <-asink.AsyncPush(items)
	}
}
