package proto

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_pusher "github.com/transferia/transferia/pkg/providers/s3/pusher"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
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
	atomicTotalSize := atomic.Uint64{}
	var mutex sync.Mutex
	var randomReader s3raw.S3RawReader
	var randomKey *string

	eg := errgroup.Group{}
	eg.SetLimit(8)
	for _, file := range files {
		eg.Go(
			func() error {
				var size uint64
				if file.Size != nil {
					size = uint64(*file.Size)
				} else {
					r.logger.Warnf("size of file %s is unknown, will measure", *file.Key)

					s3RawReader, err := r.newS3RawReader(ctx, *file.Key)
					if err != nil {
						return xerrors.Errorf("proto.estimateRows.newS3RawReader file: %s, err: %w", *file.Key, err)
					}
					defer s3RawReader.Close()

					size = uint64(s3RawReader.Size())

					mutex.Lock()
					defer mutex.Unlock()

					if randomReader == nil && size != 0 {
						randomReader = s3RawReader
						randomKey = file.Key
					}
				}
				atomicTotalSize.Add(size)
				return nil
			},
		)
	}
	if err := eg.Wait(); err != nil {
		return 0, xerrors.Errorf("proto.estimateRows.errgroup err: %w", err)
	}

	totalSize := atomicTotalSize.Load()
	if totalSize == 0 || randomKey == nil {
		return 0, nil
	}

	linesCount, err := countLines(ctx, r, *randomKey)
	if err != nil {
		return 0, xerrors.Errorf("proto.estimateRows.countLines err: %w", err)
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

	sch, readerErr := s3_reader.ExecuteSchemaResolver(ctx, r.schemaResolver)
	if readerErr != nil {
		return 0, xerrors.Errorf("unable to resolve schema for estimate: %w", readerErr)
	}

	if err := r.Read(ctx, sch, key, s3_pusher.NewSynchronousPusher(counter)); err != nil {
		return 0, xerrors.Errorf("unable to read from key: %s, err: %w", key, err)
	}

	return res, nil
}
