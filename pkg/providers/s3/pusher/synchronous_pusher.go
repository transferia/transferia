package pusher

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type SynchronousPusher struct {
	pusher abstract.Pusher
}

func (p *SynchronousPusher) IsEmpty() bool {
	return false
}

func (p *SynchronousPusher) Push(_ context.Context, chunk Chunk) error {
	if err := p.pusher(chunk.Items); err != nil {
		return xerrors.Errorf("failed to push: %w", err)
	}
	return nil
}

func (p *SynchronousPusher) Ack(chunk Chunk) (bool, error) {
	// should not be used anyway
	return false, nil
}

func NewSynchronousPusher(pusher abstract.Pusher) *SynchronousPusher {
	return &SynchronousPusher{
		pusher: pusher,
	}
}
