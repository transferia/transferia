package table_part_provider

import (
	"context"
	"sync"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"go.ytsaurus.tech/library/go/core/log"
)

const asyncBufferSize = 100_000

type SingleWorkerTPPFullAsync struct {
	mu       sync.RWMutex
	isClosed bool
	partsCh  chan (*abstract.OperationTablePart)
}

func (p *SingleWorkerTPPFullAsync) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	select {
	case parts := <-p.partsCh:
		return parts, nil
	case <-ctx.Done():
		logger.Log.Error("SingleWorkerTPPFullAsync table_part_provider cancelled", log.Error(ctx.Err()))
		return nil, xerrors.Errorf("SingleWorkerTPPFullAsync table_part_provider cancelled: %w", ctx.Err())
	}
}

func (p *SingleWorkerTPPFullAsync) AsyncLoadPartsIfNeeded(
	ctx context.Context,
	storage abstract.Storage,
	tables []abstract.TableDescription,
	cp coordinator.Coordinator,
	transferID string,
	operationID string,
	checkLoaderError func() error,
) error {
	return asyncLoadParts(ctx, storage.(abstract.AsyncOperationPartsStorage), tables, p, cp, transferID, operationID, checkLoaderError)
}

func (p *SingleWorkerTPPFullAsync) AppendParts(ctx context.Context, parts []*abstract.OperationTablePart) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.isClosed {
		return xerrors.New("SingleWorkerTPPFullAsync table_part_provider is closed")
	}
	for _, part := range parts {
		select {
		case p.partsCh <- part:
		case <-ctx.Done():
			logger.Log.Error("Append PartsCh context canceled, stopping", log.Error(ctx.Err()))
			return xerrors.Errorf("append PartsCh context canceled: %w", ctx.Err())
		}
	}
	return nil
}

func (p *SingleWorkerTPPFullAsync) AllPartsOrNil() []*abstract.OperationTablePart {
	return nil
}

func (p *SingleWorkerTPPFullAsync) EnrichShardedState(inState string) (string, error) {
	return "", xerrors.New("never should be called")
}

// Close is async-specific thread-safe and could be called many times (only first call makes sense).
func (p *SingleWorkerTPPFullAsync) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.isClosed {
		close(p.partsCh)
		p.isClosed = true
	}
}

func NewSingleWorkerTPPFullAsync() *SingleWorkerTPPFullAsync {
	return &SingleWorkerTPPFullAsync{
		mu:       sync.RWMutex{},
		isClosed: false,
		partsCh:  make(chan *abstract.OperationTablePart, asyncBufferSize),
	}
}
