package buffer

import (
	"bytes"
	"context"
)

type BufferPool struct {
	pool chan *bytes.Buffer
	size int
}

// NewBufferPool creates a new buffer pool with the given size
// If size is 0, it will create a pool with size 1 to avoid deadlock
func NewBufferPool(size int) *BufferPool {
	if size == 0 {
		size = 1
	}
	pool := &BufferPool{
		pool: make(chan *bytes.Buffer, size),
		size: size,
	}

	for i := 0; i < size; i++ {
		pool.pool <- &bytes.Buffer{}
	}

	return pool
}

// Get returns a buffer from the pool and resets it
func (p *BufferPool) Get(ctx context.Context) *bytes.Buffer {
	select {
	case <-ctx.Done():
		return nil
	case buf := <-p.pool:
		buf.Reset()
		return buf
	}
}

// Put returns the buffer to the pool and resets it
func (p *BufferPool) Put(ctx context.Context, buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	buf.Reset()
	select {
	case <-ctx.Done():
		return
	case p.pool <- buf:
	}
}
