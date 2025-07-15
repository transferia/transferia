package tasks

import (
	"context"
	"sync"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	"go.ytsaurus.tech/library/go/core/log"
)

const asyncBufferSize = 100_000

type localTablePartProvider struct {
	parts    chan (*model.OperationTablePart)
	isClosed bool
	mu       sync.RWMutex
}

func (l *localTablePartProvider) TablePartProvider() TablePartProvider {
	return func(ctx context.Context) (*model.OperationTablePart, error) {
		select {
		case parts := <-l.parts:
			return parts, nil
		case <-ctx.Done():
			logger.Log.Error("Local part provider cancelled", log.Error(ctx.Err()))
			return nil, xerrors.Errorf("local part provider cancelled: %w", ctx.Err())
		}
	}
}

func (l *localTablePartProvider) AppendParts(ctx context.Context, parts []*model.OperationTablePart) error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.isClosed {
		return xerrors.New("local table part provider is closed")
	}
	for _, part := range parts {
		select {
		case l.parts <- part:
		case <-ctx.Done():
			logger.Log.Error("Append parts context canceled, stopping", log.Error(ctx.Err()))
			return xerrors.Errorf("append parts context canceled: %w", ctx.Err())
		}
	}
	return nil
}

// Close is async-specific thread-safe and could be called many times (only first call makes sense).
func (l *localTablePartProvider) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.isClosed {
		close(l.parts)
		l.isClosed = true
	}
}

// NewLocalTablePartProvider returns provider with static parts, call of AppendParts() will return error.
func NewLocalTablePartProvider(parts ...*model.OperationTablePart) *localTablePartProvider {
	partsCh := make(chan *model.OperationTablePart, len(parts))
	for _, part := range parts {
		partsCh <- part
	}
	provider := &localTablePartProvider{
		parts:    partsCh,
		isClosed: false,
		mu:       sync.RWMutex{},
	}
	provider.Close()
	return provider
}

func NewAsyncLocalTablePartProvider() *localTablePartProvider {
	return &localTablePartProvider{
		parts:    make(chan *model.OperationTablePart, asyncBufferSize),
		isClosed: false,
		mu:       sync.RWMutex{},
	}
}
