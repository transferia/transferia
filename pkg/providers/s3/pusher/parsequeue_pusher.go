package pusher

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/parsequeue"
	"go.ytsaurus.tech/library/go/core/log"
)

type ParsequeuePusher struct {
	queue *parsequeue.ParseQueue[Chunk]
	state *pusherState
}

func (p *ParsequeuePusher) IsEmpty() bool {
	return p.state.IsEmpty()
}

func (p *ParsequeuePusher) Push(ctx context.Context, chunk Chunk) error {
	p.state.waitLimits(ctx) // slow down pushing if limit is reached
	p.state.addInflight(chunk.Size)
	p.state.setPushProgress(chunk.FilePath, chunk.Offset, chunk.Completed)
	if err := p.queue.Add(chunk); err != nil {
		return xerrors.Errorf("failed to push to parsequeue: %w", err)
	}
	return nil
}

func (p *ParsequeuePusher) Ack(chunk Chunk) (bool, error) {
	p.state.reduceInflight(chunk.Size)
	return p.state.ackPushProgress(chunk.FilePath, chunk.Offset, chunk.Completed)
}

func NewParsequeuePusher(queue *parsequeue.ParseQueue[Chunk], logger log.Logger, inflightLimit int64) *ParsequeuePusher {
	return &ParsequeuePusher{
		queue: queue,
		state: newPusherState(logger, inflightLimit),
	}
}
