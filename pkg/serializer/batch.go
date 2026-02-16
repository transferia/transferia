package serializer

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"sync"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/serializer/buffer"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

const (
	DefaultBatchSerializerThreshold = 25000
)

type BatchSerializerConfig struct {
	Concurrency int
	Threshold   int

	DisableConcurrency bool
}

type batchSerializer struct {
	serializer Serializer
	separator  []byte

	concurrency int
	threshold   int

	bufferPool *buffer.BufferPool
}

func newBatchSerializer(s Serializer, sep []byte, config *BatchSerializerConfig) BatchSerializer {
	c := config
	if c == nil {
		c = new(BatchSerializerConfig)
	}

	if c.DisableConcurrency {
		return &batchSerializer{
			serializer:  s,
			separator:   sep,
			concurrency: 1,
			threshold:   0,
			bufferPool:  buffer.NewBufferPool(1),
		}
	}

	concurrency := c.Concurrency
	if concurrency == 0 {
		concurrency = runtime.GOMAXPROCS(0)
	}

	threshold := c.Threshold
	if threshold == 0 {
		threshold = DefaultBatchSerializerThreshold
	}

	return &batchSerializer{
		serializer:  s,
		separator:   sep,
		concurrency: concurrency,
		threshold:   threshold,
		bufferPool:  buffer.NewBufferPool(concurrency),
	}
}

func (s *batchSerializer) Serialize(items []*abstract.ChangeItem) ([]byte, error) {
	ctx := context.Background()
	if s.concurrency < 2 || len(items) <= s.threshold {
		data, err := s.serialize(ctx, items)
		if err != nil {
			return nil, xerrors.Errorf("batchSerializer: %w", err)
		}

		return data, nil
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(s.concurrency)

	bufs := make([][]byte, (len(items)+s.threshold-1)/s.threshold)
	for i := range bufs {
		if ctx.Err() != nil {
			break
		}

		i := i
		start := i * s.threshold
		end := min(start+s.threshold, len(items))
		g.Go(func() error {
			data, err := s.serialize(ctx, items[start:end])
			if err != nil {
				return err
			}

			bufs[i] = data
			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, xerrors.Errorf("batchSerializer: %w", err)
	}

	// trim last separator if present
	joinedBuffers := bytes.Join(bufs, s.separator)
	joinedBuffers = bytes.TrimSuffix(joinedBuffers, s.separator)

	return joinedBuffers, nil
}

func (s *batchSerializer) SerializeAndWrite(ctx context.Context, items []*abstract.ChangeItem, writer io.Writer) (int, error) {
	if s.concurrency < 2 || len(items) <= s.threshold {
		buf := s.bufferPool.Get(ctx)
		defer s.bufferPool.Put(ctx, buf)
		if err := s.serializeToBuffer(ctx, items, buf); err != nil {
			return 0, xerrors.Errorf("batchSerializer: unable to serialize items: %w", err)
		}
		written, err := writer.Write(buf.Bytes())
		if err != nil {
			return 0, xerrors.Errorf("batchSerializer: unable to write: %w", err)
		}

		return written, nil
	}

	cancelableCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	eg, egCtx := errgroup.WithContext(cancelableCtx)
	eg.SetLimit(s.concurrency)

	var nextToWrite int
	cond := sync.NewCond(&sync.Mutex{})

	go func() {
		<-egCtx.Done()
		cond.L.Lock()
		cond.Broadcast()
		cond.L.Unlock()
	}()

	totalWritten := atomic.NewInt32(0)

	bufsCnt := (len(items) + s.threshold - 1) / s.threshold
	for i := range bufsCnt {
		if egCtx.Err() != nil {
			return 0, xerrors.Errorf("batchSerializer: context error: %w", egCtx.Err())
		}

		i := i
		start := i * s.threshold
		end := min(start+s.threshold, len(items))

		eg.Go(func() error {
			buf := s.bufferPool.Get(egCtx)
			if buf == nil {
				return xerrors.Errorf("batchSerializer: nil buffer from pool")
			}
			defer s.bufferPool.Put(egCtx, buf)

			if err := s.serializeToBuffer(egCtx, items[start:end], buf); err != nil {
				return xerrors.Errorf("batchSerializer: unable to serialize items: %w", err)
			}

			// add separator between parts
			if i != bufsCnt-1 && len(s.separator) > 0 {
				buf.Write(s.separator)
			}

			// wait for previous part to be written
			cond.L.Lock()
			defer cond.L.Unlock()
			for nextToWrite != i && egCtx.Err() == nil {
				cond.Wait()
			}
			if egCtx.Err() != nil {
				return xerrors.Errorf("batchSerializer: context canceled: %w", egCtx.Err())
			}
			written, err := writer.Write(buf.Bytes())
			if err != nil {
				return xerrors.Errorf("batchSerializer: unable to write serialized part: %w", err)
			}
			totalWritten.Add(int32(written))

			nextToWrite++
			cond.Broadcast()

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return 0, xerrors.Errorf("batchSerializer: processing error: %w", err)
	}

	return int(totalWritten.Load()), nil
}

func (s *batchSerializer) serializeToBuffer(ctx context.Context, items []*abstract.ChangeItem, out *bytes.Buffer) error {
	if len(items) == 0 {
		return nil
	}
	for _, item := range items[:len(items)-1] {
		if err := s.serializer.SerializeWithSeparatorTo(item, s.separator, out); err != nil {
			return xerrors.Errorf("unable to serialize item: %w", err)
		}
	}
	if err := s.serializer.SerializeWithSeparatorTo(items[len(items)-1], nil, out); err != nil {
		return xerrors.Errorf("unable to serialize last item: %w", err)
	}
	return nil
}

func (s *batchSerializer) serialize(ctx context.Context, items []*abstract.ChangeItem) ([]byte, error) {
	var buf bytes.Buffer
	if err := s.serializeToBuffer(ctx, items, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *batchSerializer) Close() ([]byte, error) {
	return s.serializer.Close()
}
