package bufferer

import (
	"github.com/transferia/transferia/pkg/abstract"
)

type buffer struct {
	// batches represents all batches of items added to buffer.
	// It is stored as slice of batches instead of slice of changeitems to prevent
	// unnecessary `growSlice` functions and reallocations on `buffer.Add` calls.
	batches         [][]abstract.ChangeItem
	size            int // size is number of all items from batches.
	batchValuesSize uint64
	errChs          []chan error
}

func newBuffer() *buffer {
	return &buffer{
		batches:         nil,
		size:            0,
		batchValuesSize: 0,
		errChs:          make([]chan error, 0),
	}
}

func (b *buffer) Add(items []abstract.ChangeItem, errCh chan error) {
	b.batches = append(b.batches, items)
	b.size += len(items)
	b.batchValuesSize += valuesSizeOf(items)
	b.errChs = append(b.errChs, errCh)
}

func (b *buffer) Flush(flushFn func([]abstract.ChangeItem) error) {
	var totalBatch []abstract.ChangeItem // totalBatch is one slice of all changeitems from batches.
	if len(b.batches) == 1 {
		totalBatch = b.batches[0] // To not copy changeitems.
	} else {
		totalBatch = make([]abstract.ChangeItem, 0, b.size)
		for _, items := range b.batches {
			totalBatch = append(totalBatch, items...)
		}
	}

	var err error = nil
	if len(totalBatch) > 0 {
		err = flushFn(totalBatch)
	}
	for _, c := range b.errChs {
		c <- err
	}

	b.batches = nil
	b.size = 0
	b.batchValuesSize = 0
	b.errChs = make([]chan error, 0)
}

func (b *buffer) Len() int {
	return b.size
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
