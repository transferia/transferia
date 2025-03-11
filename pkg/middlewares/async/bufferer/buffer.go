package bufferer

import "github.com/transferia/transferia/pkg/abstract"

type buffer struct {
	batch           []abstract.ChangeItem
	batchValuesSize uint64
	errChs          []chan error
}

func newBuffer() *buffer {
	return &buffer{
		batch:           make([]abstract.ChangeItem, 0),
		batchValuesSize: 0,
		errChs:          make([]chan error, 0),
	}
}

func (b *buffer) Add(items []abstract.ChangeItem, errCh chan error) {
	b.batch = append(b.batch, items...)
	b.batchValuesSize += valuesSizeOf(items)
	b.errChs = append(b.errChs, errCh)
}

func (b *buffer) Flush(flushFn func([]abstract.ChangeItem) error) {
	var err error = nil
	if len(b.batch) > 0 {
		err = flushFn(b.batch)
	}
	for _, c := range b.errChs {
		c <- err
	}

	b.batch = make([]abstract.ChangeItem, 0)
	b.batchValuesSize = 0
	b.errChs = make([]chan error, 0)
}

func (b *buffer) Len() int {
	return len(b.batch)
}

func (b *buffer) ValuesSize() uint64 {
	return b.batchValuesSize
}

func valuesSizeOf(items []abstract.ChangeItem) uint64 {
	result := uint64(0)
	for _, item := range items {
		result += item.Size.Values
	}
	return result
}
