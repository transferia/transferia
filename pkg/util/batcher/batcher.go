package batcher

import (
	"slices"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
)

type Batcher[T any] struct {
	batchSize int
	pusher    func([]T) error

	dataLen int
	data    [][]T
}

func NewBatcher[T any](batchSize int, pusher func([]T) error) *Batcher[T] {
	return &Batcher[T]{
		batchSize: batchSize,
		pusher:    pusher,

		dataLen: 0,
		data:    nil,
	}
}

// Close removes all data stored in buffer and invalidate batcher.
func (b *Batcher[T]) Close() {
	b.batchSize = 1
	b.pusher = func([]T) error { return xerrors.New("batcher is closed") }
	b.data = nil
	b.dataLen = 0
}

// FlushAndClose pushes all data stored in buffer and invalidate batcher.
func (b *Batcher[T]) FlushAndClose() error {
	err := b.Flush()
	b.Close()
	return err
}

func (b *Batcher[T]) Flush() error {
	if b.dataLen > b.batchSize {
		if err := b.tryFlush(); err != nil {
			return xerrors.Errorf("unable to flush many batches: %w", err)
		}
	}
	if b.dataLen == 0 {
		return nil
	}
	toPusher := slices.Concat(b.data...)
	b.data = nil
	b.dataLen = 0
	return b.pusher(toPusher)
}

func (b *Batcher[T]) Append(values []T) error {
	if b.batchSize < 1 {
		if err := b.pusher(values); err != nil {
			return xerrors.Errorf("Batcher with no limit push of %d values failed: %w", len(values), err)
		}
		return nil
	}
	if len(values) == 0 {
		return nil
	}
	b.data = append(b.data, values)
	b.dataLen += len(values)
	return b.tryFlush()
}

// tryFlush calls pusher only with batchSize batches.
func (b *Batcher[T]) tryFlush() error {
	for b.dataLen >= b.batchSize {
		if b.dataLen == b.batchSize {
			toPusher := slices.Concat(b.data...)
			b.data = nil
			b.dataLen = 0
			return b.pusher(toPusher)
		}
		toPusher := make([]T, 0, b.batchSize)
		for len(toPusher) < b.batchSize {
			for i, slice := range b.data {
				needed := b.batchSize - len(toPusher)
				if len(slice) <= needed {
					toPusher = append(toPusher, slice...)
					b.data[i] = nil
				} else {
					toPusher = append(toPusher, slice[:needed]...)
					b.data[i] = slice[needed:]
				}
			}
		}
		b.removeEmptyFromData()
		b.dataLen -= len(toPusher)
		if err := b.pusher(toPusher); err != nil {
			return xerrors.Errorf("push of %d items failed: %w", len(toPusher), err)
		}
	}
	return nil
}

func (b *Batcher[T]) removeEmptyFromData() {
	b.data = yslices.Reduce(b.data, func(x []T) bool { return len(x) > 0 })
}
