package pusher

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type SyncPusher struct {
	pusher abstract.Pusher
}

func (p *SyncPusher) IsEmpty() bool {
	return false
}

func (p *SyncPusher) Push(_ context.Context, chunk Chunk) error {
	if err := p.pusher(chunk.Items); err != nil {
		return xerrors.Errorf("failed to push: %w", err)
	}
	return nil
}

func (p *SyncPusher) Ack(chunk Chunk) (bool, error) {
	// should not be used anyway
	return false, nil
}

func NewSyncPusher(pusher abstract.Pusher) *SyncPusher {
	return &SyncPusher{
		pusher: pusher,
	}
}
