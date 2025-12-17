package pusher

import (
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsequeue"
	"go.ytsaurus.tech/library/go/core/log"
)

func New(pusher abstract.Pusher, queue *parsequeue.ParseQueue[Chunk], logger log.Logger, inflightLimit int64) Pusher {
	if queue != nil {
		return NewParsequeuePusher(queue, logger, inflightLimit)
	} else {
		return NewSynchronousPusher(pusher)
	}
}
