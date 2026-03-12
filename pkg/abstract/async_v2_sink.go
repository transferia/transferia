package abstract

import (
	"context"
	"io"
)

type AsyncPushResult interface {
	GetResult() any
	GetError() error
}

type QueueToS3Sink interface {
	io.Closer
	AsyncV2Push(ctx context.Context, errCh chan<- AsyncPushResult, items []ChangeItem)
}

type QueueSourceAsyncPushResult struct {
	Result QueueResult
	Err    error
}

type QueueResult struct {
	Offsets []uint64
}

func (r *QueueSourceAsyncPushResult) GetError() error { return r.Err }
func (r *QueueSourceAsyncPushResult) GetResult() any  { return r.Result }
