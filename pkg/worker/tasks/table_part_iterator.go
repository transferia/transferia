package tasks

import (
	"context"
	"sync"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/worker/tasks/table_part_provider"
)

type tablePartIterator struct {
	mu     sync.Mutex
	getter table_part_provider.AbstractTablePartProviderGetter
	done   bool
}

func newTablePartIterator(getter table_part_provider.AbstractTablePartProviderGetter) *tablePartIterator {
	return &tablePartIterator{
		mu:     sync.Mutex{},
		getter: getter,
		done:   false,
	}
}

func (i *tablePartIterator) Next(ctx context.Context) (*abstract.OperationTablePart, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if ctx.Err() != nil || i.done {
		return nil, nil
	}
	part, err := i.getter.NextOperationTablePart(ctx)
	if err != nil || part == nil {
		i.done = true
	}
	return part, err
}
