package proto

import (
	"context"
	"math"
	"sync/atomic"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	chunk_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"golang.org/x/sync/errgroup"
)

// estimateRows calculates approximate rows count of files.
//
// Implementation:
// 1. Open readers for all files to obtain their sizes.
// 2. Take one random reader, calculate its average line size.
// 3. Divide size of all files by average line size to get result (total rows count).
func estimateRows(ctx context.Context, r *ProtoReader, files []*aws_s3.Object) (uint64, error) {
	atomicTotalSize := atomic.Int64{}
	var randomReader s3raw.S3RawReader
	var randomKey *string

	eg := errgroup.Group{}
	eg.SetLimit(8)
	for _, file := range files {
		eg.Go(func() error {
			var size int64
			if file.Size != nil {
				size = *file.Size
			} else {
				r.logger.Warnf("size of file %s is unknown, will measure", *file.Key)
				s3RawReader, err := r.newS3RawReader(ctx, *file.Key)
				if err != nil {
					return xerrors.Errorf("unable to open s3RawReader for file: %s: %w", *file.Key, err)
				}
				size = s3RawReader.Size()
				if randomReader == nil && size > 0 {
					randomReader = s3RawReader
					randomKey = file.Key
				}
			}
			atomicTotalSize.Add(size)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return 0, xerrors.Errorf("unable to open readers: %w", err)
	}

	totalSize := atomicTotalSize.Load()
	if totalSize == 0 || randomKey == nil {
		return 0, nil
	}

	linesCount, err := countLines(ctx, r, *randomKey)
	if err != nil {
		return 0, xerrors.Errorf("unable to parse: %w", err)
	}
	bytesPerLine := float64(randomReader.Size()) / float64(linesCount)
	totalLines := math.Ceil(float64(totalSize) / bytesPerLine)

	return uint64(totalLines), nil
}

func countLines(ctx context.Context, r *ProtoReader, key string) (int, error) {
	res := 0

	counter := func(items []abstract.ChangeItem) error {
		res += len(items)
		return nil
	}

	if err := r.Read(ctx, key, chunk_pusher.NewSyncPusher(counter)); err != nil {
		return 0, xerrors.Errorf("unable to read file '%s': %w", key, err)
	}

	return res, nil
}
