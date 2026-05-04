package s3raw

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type wrapper[T io.ReadCloser] func(io.Reader) (T, error)

var (
	_ io.ReaderAt = (*wrappedReader[io.ReadCloser])(nil)
	_ io.Reader   = (*wrappedReader[io.ReadCloser])(nil)
	_ ReaderAll   = (*wrappedReader[io.ReadCloser])(nil)
)

type wrappedReader[T io.ReadCloser] struct {
	fetcher                *s3Fetcher
	client                 s3iface.S3API
	stats                  *stats.SourceStats
	fullUncompressedObject []byte
	currReader             T
	closeFunc              []func() error
	wrapper                wrapper[T]
}

var (
	_ ReaderAll   = (*wrappedReader[io.ReadCloser])(nil)
	_ S3RawReader = (*wrappedReader[io.ReadCloser])(nil)
)

func (r *wrappedReader[T]) ReadAt(buffer []byte, offset int64) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if r.fullUncompressedObject == nil {
		if err := r.loadObjectInMemory(); err != nil {
			return 0, xerrors.Errorf("unable to loadObjectInMemory, err: %w", err)
		}
	}

	totalSize := int64(len(r.fullUncompressedObject))
	start, end, err := calcRange(int64(len(buffer)), offset, totalSize)
	if err != nil {
		// Return EOF / UnexpectedEOF as-is without wrapping,
		// to preserve io.ReaderAt contract semantics.
		if xerrors.Is(err, io.EOF) || xerrors.Is(err, io.ErrUnexpectedEOF) {
			n := copy(buffer, r.fullUncompressedObject[start:end+1])
			r.stats.Size.Add(int64(n))
			return n, err
		}

		// Wrap transport/internal errors with context.
		return 0, xerrors.Errorf("unable to calcRange, err: %w", err)
	}

	// Adjust buffer size to the available range if needed.
	if int64(len(buffer)) > end-start+1 {
		buffer = buffer[:end-start+1]
	}

	n := copy(buffer, r.fullUncompressedObject[start:end+1])
	r.stats.Size.Add(int64(n))

	return n, nil
}

func (r *wrappedReader[T]) Read(buffer []byte) (int, error) {
	if r.closeFunc == nil {
		if err := r.startStreamReader(); err != nil {
			return 0, xerrors.Errorf("unable to startStreamReader, err: %w", err)
		}
	}

	return r.currReader.Read(buffer)
}

func (r *wrappedReader[T]) startStreamReader() error {
	rawReader, err := r.fetcher.makeReader()
	if err != nil {
		return xerrors.Errorf("unable to makeReader, err: %w", err)
	}
	r.closeFunc = append(r.closeFunc, rawReader.Close)

	currWrappedReader, err := r.wrapper(rawReader)
	if err != nil {
		return xerrors.Errorf("unable to wrapReader, err: %w", err)
	}
	r.currReader = currWrappedReader

	r.closeFunc = append(r.closeFunc, currWrappedReader.Close)
	return nil
}

func (r *wrappedReader[T]) Close() error {
	defer func() {
		r.closeFunc = nil
	}()

	if r.closeFunc == nil {
		return nil
	}
	for _, currCloseFunc := range r.closeFunc {
		if err := currCloseFunc(); err != nil {
			return xerrors.Errorf("unable to close, err: %w", err)
		}
	}
	return nil
}

func (r *wrappedReader[T]) ReadAll(ctx context.Context) ([]byte, error) {
	file, err := r.client.GetObjectWithContext(
		ctx,
		&aws_s3.GetObjectInput{
			Bucket: aws.String(r.fetcher.bucket),
			Key:    aws.String(r.fetcher.key),
		},
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to GetObject, err: %w", err)
	}
	defer func() {
		if err := file.Body.Close(); err != nil {
			logger.Log.Warnf("Unable to close body of %s: %s", r.fetcher.key, err.Error())
		}
	}()

	compressedSize := int64(0)
	if file.ContentLength != nil {
		compressedSize = *file.ContentLength
	}

	currWrapper, err := r.wrapper(file.Body)
	if err != nil {
		return nil, xerrors.Errorf("unable to wrapReader, err: %w", err)
	}
	defer currWrapper.Close()

	uncompressedSize := int64(0)
	if meta, ok := file.Metadata["uncompressed-size"]; ok && meta != nil {
		// https://docs-go.hexacode.org/pkg/strconv
		// the value corresponding to s cannot be represented by a signed integer of the given size, err.Err = ErrRange
		// and the returned value is the maximum magnitude integer of the appropriate bitSize and sign.
		currUncompressedSize, err := strconv.ParseInt(*meta, 10, 64)
		if err != nil {
			logger.Log.Warn(fmt.Sprintf("Unable to parse size '%s' from metadata", *meta), log.Error(err))
		} else {
			uncompressedSize = currUncompressedSize
		}
	}

	res := bytes.NewBuffer(make([]byte, 0, max(uncompressedSize, compressedSize)))
	_, err = io.Copy(res, currWrapper)
	if err != nil {
		return nil, xerrors.Errorf("unable to io.Copy, err: %w", err)
	}
	return res.Bytes(), nil
}

func (r *wrappedReader[T]) loadObjectInMemory() error {
	ctx := context.Background()
	var err error
	r.fullUncompressedObject, err = r.ReadAll(ctx)
	return err
}

func (r *wrappedReader[T]) LastModified() time.Time {
	return r.fetcher.lastModified()
}

func (r *wrappedReader[T]) Size() int64 {
	sz := r.fetcher.size()
	if sz > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(sz)
}

func newWrappedReader[T io.ReadCloser](
	fetcher *s3Fetcher,
	client s3iface.S3API,
	stats *stats.SourceStats,
	wrapper wrapper[T],
) (S3RawReader, error) {
	if fetcher == nil {
		return nil, xerrors.New("missing s3 fetcher for wrapped reader")
	}
	if client == nil {
		return nil, xerrors.New("missing s3 client for wrapped reader")
	}
	if stats == nil {
		return nil, xerrors.New("missing stats for wrapped reader")
	}
	return &wrappedReader[T]{
		fetcher:                fetcher,
		client:                 client,
		stats:                  stats,
		fullUncompressedObject: nil,
		currReader:             *new(T),
		closeFunc:              nil,
		wrapper:                wrapper,
	}, nil
}
