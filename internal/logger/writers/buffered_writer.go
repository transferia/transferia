package writers

import (
	"bytes"
	"context"
	"sync"
	"time"
)

type BufferedWriter struct {
	writer    Writer
	rw        sync.Mutex
	buf       bytes.Buffer
	maxMemory int
}

func (w *BufferedWriter) Write(ctx context.Context, p []byte) error {
	w.rw.Lock()
	defer w.rw.Unlock()
	w.buf.Write(p)

	if w.buf.Len() > 1024 {
		return w.flushBuffer(ctx)
	}
	return nil
}

func (w *BufferedWriter) Close() error {
	w.rw.Lock()
	defer w.rw.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = w.flushBuffer(ctx)

	return w.writer.Close()
}

func (w *BufferedWriter) flushBuffer(ctx context.Context) error {
	combined := w.buf.Bytes()
	if string(combined)[w.buf.Len()-1] != '\n' {
		return nil
	}

	if err := w.writer.Write(ctx, combined); err != nil {
		return err
	}
	w.buf.Reset()

	return nil
}

func NewBufferedWriter(writer Writer, maxMemory int) *BufferedWriter {
	return &BufferedWriter{
		writer:    writer,
		rw:        sync.Mutex{},
		buf:       bytes.Buffer{},
		maxMemory: maxMemory,
	}
}
