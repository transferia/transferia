package serializer

import (
	"bytes"
	"context"
	"runtime"

	"github.com/transferia/transferia/pkg/abstract"
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
	}
}

func (s *batchSerializer) Serialize(items []*abstract.ChangeItem) ([]byte, error) {
	if s.concurrency < 2 || len(items) <= s.threshold {
		data, err := s.serialize(items)
		if err != nil {
			return nil, xerrors.Errorf("batchSerializer: %w", err)
		}

		return data, nil
	}

	g, ctx := errgroup.WithContext(context.TODO())
	g.SetLimit(s.concurrency)

	bufs := make([][]byte, (len(items)+s.threshold-1)/s.threshold)
	for i := range bufs {
		if ctx.Err() != nil {
			break
		}

		i := i
		g.Go(func() error {
			end := (i + 1) * s.threshold
			if end > len(items) {
				end = len(items)
			}

			data, err := s.serialize(items[i*s.threshold : end])
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

	return bytes.Join(bufs, s.separator), nil
}

func (s *batchSerializer) serialize(items []*abstract.ChangeItem) ([]byte, error) {
	var out []byte

	for i, item := range items {
		data, err := s.serializer.Serialize(item)
		if err != nil {
			return nil, err
		}

		if i > 0 && len(s.separator) > 0 {
			out = append(out, s.separator...)
		}

		out = append(out, data...)
	}

	return out, nil
}
