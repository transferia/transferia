package container

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

func TestNewContextReader(t *testing.T) {
	// Create a simple reader and context factory
	reader := strings.NewReader("test")
	ctxFactory := func() (context.Context, context.CancelFunc) {
		return context.WithCancel(context.Background())
	}

	// Test creation
	cr := NewContextReader(reader, ctxFactory)
	require.NotNil(t, cr)
}

func TestNewContextReader_NoContextFactory(t *testing.T) {
	// Create a simple reader and context factory
	reader := strings.NewReader("test")

	// Test creation
	cr := NewContextReader(reader, nil)
	require.NotNil(t, cr)
}

func TestContextReader_Read(t *testing.T) {
	t.Run("no context factory", func(t *testing.T) {
		reader := strings.NewReader("nil test data")
		cr := NewContextReader(reader, nil)

		buf := make([]byte, 3)
		n, err := cr.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, "nil", string(buf))
	})

	t.Run("normal read", func(t *testing.T) {
		reader := strings.NewReader("test data")
		ctxFactory := func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		}
		cr := NewContextReader(reader, ctxFactory)

		buf := make([]byte, 4)
		n, err := cr.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, "test", string(buf))
	})

	t.Run("context cancellation", func(t *testing.T) {
		// Create a slow reader that will block
		slowReader := &slowReader{duration: 100 * time.Millisecond}
		ctx, cancel := context.WithCancel(context.Background())
		ctxFactory := func() (context.Context, context.CancelFunc) { return ctx, cancel }

		cr := NewContextReader(slowReader, ctxFactory)

		// Cancel context after a short delay
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		buf := make([]byte, 10)
		_, err := cr.Read(buf)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("reader error propagation", func(t *testing.T) {
		// Create a reader that returns an error
		errReader := &errorReader{err: errors.New("read error")}
		ctxFactory := func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		}
		cr := NewContextReader(errReader, ctxFactory)

		buf := make([]byte, 10)
		_, err := cr.Read(buf)
		require.Error(t, err)
		require.Equal(t, "read error", err.Error())
	})

	t.Run("context error propagation", func(t *testing.T) {
		cancelCauseErr := xerrors.Errorf("funky context error")

		// Create a slow reader that will block
		slowReader := &slowReader{duration: 100 * time.Millisecond}
		ctx, cancel := context.WithCancelCause(context.Background())
		ctxFactory := func() (context.Context, context.CancelFunc) { return ctx, func() {} }

		cr := NewContextReader(slowReader, ctxFactory)

		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel(cancelCauseErr)
		}()

		buf := make([]byte, 10)
		_, err := cr.Read(buf)
		require.ErrorIs(t, err, cancelCauseErr)
	})
}

// slowReader is a reader that takes time to read
type slowReader struct {
	duration time.Duration
}

func (r *slowReader) Read(p []byte) (int, error) {
	time.Sleep(r.duration)
	return 0, nil
}

// errorReader is a reader that always returns an error
type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (int, error) {
	return 0, r.err
}
