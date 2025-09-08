package table_part_provider

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
)

type SingleWorkerTPPFullSync struct {
	allParts   []*abstract.OperationTablePart
	leastParts []*abstract.OperationTablePart
}

func (p *SingleWorkerTPPFullSync) AppendParts(_ context.Context, parts []*abstract.OperationTablePart) error {
	p.allParts = append(p.allParts, parts...)
	p.leastParts = append(p.leastParts, parts...)
	return nil
}

func (p *SingleWorkerTPPFullSync) NextOperationTablePart(ctx context.Context) (*abstract.OperationTablePart, error) {
	if len(p.leastParts) == 0 {
		return nil, nil
	} else {
		result := p.leastParts[0]
		p.leastParts = p.leastParts[1:]
		return result, nil
	}
}

func (p *SingleWorkerTPPFullSync) EnrichShardedState(inState string) (string, error) {
	return "", xerrors.New("never should be called")
}

func (p *SingleWorkerTPPFullSync) AsyncLoadPartsIfNeeded(
	ctx context.Context,
	storage abstract.Storage,
	tables []abstract.TableDescription,
	cp coordinator.Coordinator,
	transferID string,
	operationID string,
	checkLoaderError func() error,
) error {
	return nil
}

func (p *SingleWorkerTPPFullSync) AllPartsOrNil() []*abstract.OperationTablePart {
	return p.allParts
}

func (p *SingleWorkerTPPFullSync) Close() {}

func NewSingleWorkerTPPFullSync() *SingleWorkerTPPFullSync {
	return &SingleWorkerTPPFullSync{
		allParts:   nil,
		leastParts: nil,
	}
}
