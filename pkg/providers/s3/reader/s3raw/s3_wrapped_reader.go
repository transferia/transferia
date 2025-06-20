//go:build !disable_s3_provider

package s3raw

import (
	"bytes"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/stats"
)

type wrapper[T io.ReadCloser] func(io.Reader) (T, error)

var _ io.ReaderAt = (*wrappedReader[io.ReadCloser])(nil)

type wrappedReader[T io.ReadCloser] struct {
	fetcher                *s3Fetcher
	downloader             *s3manager.Downloader
	stats                  *stats.SourceStats
	fullUncompressedObject []byte
	wrapper                wrapper[T]
}

func (r *wrappedReader[T]) ReadAt(buffer []byte, offset int64) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if r.fullUncompressedObject == nil {
		if err := r.loadObjectInMemory(); err != nil {
			return 0, xerrors.Errorf("failed to load full file %s into memory: %w", r.fetcher.key, err)
		}
	}

	totalSize := int64(len(r.fullUncompressedObject))
	start, end, returnErr := calcRange(int64(len(buffer)), offset, totalSize)
	if returnErr != nil && !xerrors.Is(returnErr, io.EOF) {
		return 0, xerrors.Errorf("unable to calculate new read range for file %s: %w", r.fetcher.key, returnErr)
	}

	if int64(len(buffer)) > end-start+1 {
		buffer = buffer[:end-start+1] // Reduce buffer size to match range.
	}

	n := copy(buffer, r.fullUncompressedObject[start:end+1])
	r.stats.Size.Add(int64(n))
	if returnErr != nil {
		return n, xerrors.Errorf("reached EOF: %w", returnErr)
	}
	return n, nil
}

func (r *wrappedReader[T]) loadObjectInMemory() error {
	_, err := r.fetcher.fetchSize()
	if err != nil {
		return xerrors.Errorf("unable to fetch object size %s: %w", r.fetcher.key, err)
	}

	buff := aws.NewWriteAtBuffer(make([]byte, r.fetcher.objectSize))
	_, err = r.downloader.DownloadWithContext(r.fetcher.ctx, buff, &s3.GetObjectInput{
		Bucket: aws.String(r.fetcher.bucket),
		Key:    aws.String(r.fetcher.key),
	})
	if err != nil {
		return xerrors.Errorf("failed to download object %s: %w", r.fetcher.key, err)
	}

	data := buff.Bytes()

	currWrapper, err := r.wrapper(bytes.NewReader(data))
	if err != nil {
		return xerrors.Errorf("failed to initialize wrapper: %w", err)
	}
	defer currWrapper.Close()

	r.fullUncompressedObject, err = io.ReadAll(currWrapper)
	if err != nil {
		return xerrors.Errorf("failed to read from wrapper %s: %w", r.fetcher.key, err)
	}
	return nil
}

func newWrappedReader[T io.ReadCloser](
	fetcher *s3Fetcher, downloader *s3manager.Downloader, stats *stats.SourceStats, wrapper wrapper[T],
) (io.ReaderAt, error) {
	if fetcher == nil {
		return nil, xerrors.New("missing s3 fetcher for gzip reader")
	}
	if downloader == nil {
		return nil, xerrors.New("missing s3 downloader for gzip reader")
	}
	if stats == nil {
		return nil, xerrors.New("missing stats for gzip reader")
	}
	return &wrappedReader[T]{
		fetcher:                fetcher,
		downloader:             downloader,
		stats:                  stats,
		fullUncompressedObject: nil,
		wrapper:                wrapper,
	}, nil
}
