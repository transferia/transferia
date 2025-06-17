//go:build !disable_s3_provider

package pusher

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsequeue"
	"go.ytsaurus.tech/library/go/core/log"
)

type Pusher interface {
	IsEmpty() bool
	Push(ctx context.Context, chunk Chunk) error
	// Ack is used in the parsqueue pusher as a way of keeping the state of files currently being processed clean.
	// Ack has no effect in the sync pusher, here files are processed from start to finish before new ones are fetched so no state is needed.
	// Ack is called by the ack method of the parsqueue once a chunk is pushed.
	// It returns a bool that gives information if a file was fully processed and is done.
	// It errors out if more then one ack was called on the same chunk of data.
	Ack(chunk Chunk) (bool, error)
}

type Chunk struct {
	FilePath  string
	Completed bool
	Offset    any
	Size      int64
	Items     []abstract.ChangeItem
}

func New(pusher abstract.Pusher, queue *parsequeue.ParseQueue[Chunk], logger log.Logger, inflightLimit int64) Pusher {
	if queue != nil {
		return NewParsequeuePusher(queue, logger, inflightLimit)
	} else {
		return NewSyncPusher(pusher)
	}
}
