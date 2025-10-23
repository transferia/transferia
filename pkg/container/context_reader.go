package container

import (
	"context"
	"io"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

// ContextReader wraps an io.Reader with context cancellation
type ContextReader struct {
	reader         io.Reader
	contextFactory func() (context.Context, context.CancelFunc)
}

// NewContextReader creates a new cancellable reader
// contextFactory is used to generate context for every blocking `reader.Read` call in order
// to cancel it anytime
// If contextFactory is nil, context.Background() is used
func NewContextReader(reader io.Reader, contextFactory func() (context.Context, context.CancelFunc)) *ContextReader {
	return &ContextReader{reader: reader, contextFactory: contextFactory}
}

func (r *ContextReader) createContextFromFactory() (context.Context, context.CancelFunc) {
	if r.contextFactory == nil {
		return context.Background(), func() {}
	}
	return r.contextFactory()
}

// Read implements io.Reader interface with context cancellation created from the factory
func (r *ContextReader) Read(p []byte) (n int, err error) {
	type result struct {
		n   int
		err error
	}
	ch := make(chan result, 1)

	ctx, cancel := r.createContextFromFactory()
	if cancel != nil {
		defer cancel()
	}

	go func() {
		n, err := r.reader.Read(p)
		ch <- result{n, err}
	}()

	select {
	case <-ctx.Done():
		return 0, xerrors.Errorf("context cancelled in reader with cause: %w", context.Cause(ctx))
	case res := <-ch:
		return res.n, res.err
	}
}
