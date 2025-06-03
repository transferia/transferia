package pusher

import (
	"context"
	"sync"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/parsequeue"
	"go.ytsaurus.tech/library/go/core/log"
)

type ParsequeuePusher struct {
	queue *parsequeue.ParseQueue[Chunk]
	State PusherState
}

func (p *ParsequeuePusher) IsEmpty() bool {
	return p.State.IsEmpty()
}

func (p *ParsequeuePusher) Push(ctx context.Context, chunk Chunk) error {
	p.State.waitLimits(ctx) // slow down pushing if limit is reached
	p.State.addInflight(chunk.Size)
	p.State.setPushProgress(chunk.FilePath, chunk.Offset, chunk.Completed)
	if err := p.queue.Add(chunk); err != nil {
		return xerrors.Errorf("failed to push to parsequeue: %w", err)
	}

	return nil
}

func (p *ParsequeuePusher) Ack(chunk Chunk) (bool, error) {
	p.State.reduceInflight(chunk.Size)
	return p.State.ackPushProgress(chunk.FilePath, chunk.Offset, chunk.Completed)
}

func NewParsequeuePusher(queue *parsequeue.ParseQueue[Chunk], logger log.Logger, inflightLimit int64) *ParsequeuePusher {
	return &ParsequeuePusher{
		queue: queue,
		State: PusherState{
			mu:            sync.Mutex{},
			logger:        logger,
			inflightLimit: inflightLimit,
			inflightBytes: 0,
			PushProgress:  map[string]Progress{},
			counter:       0,
		},
	}
}
