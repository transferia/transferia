package s3raw

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type wrapper[T io.ReadCloser] func(io.Reader) (T, error)

var (
	_ io.ReaderAt = (*wrappedReader[io.ReadCloser])(nil)
	_ ReaderAll   = (*wrappedReader[io.ReadCloser])(nil)
)

type wrappedReader[T io.ReadCloser] struct {
	fetcher                *s3Fetcher
	client                 s3iface.S3API
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

func (r *wrappedReader[T]) ReadAll() ([]byte, error) {
	file, err := r.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(r.fetcher.bucket),
		Key:    aws.String(r.fetcher.key),
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to get object %s: %w", r.fetcher.key, err)
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
		return nil, xerrors.Errorf("failed to initialize wrapper: %w", err)
	}
	defer currWrapper.Close()

	uncompressedSize := int64(0)
	if meta, ok := file.Metadata["uncompressed-size"]; ok && meta != nil {
		if uncompressedSize, err = strconv.ParseInt(*meta, 10, 64); err != nil {
			uncompressedSize = 0
			logger.Log.Warn(fmt.Sprintf("Unable to parse size '%s' from metadata", *meta), log.Error(err))
		}
	}

	res := bytes.NewBuffer(make([]byte, 0, max(uncompressedSize, compressedSize)))
	_, err = io.Copy(res, currWrapper)
	return res.Bytes(), err
}

func (r *wrappedReader[T]) loadObjectInMemory() error {
	var err error
	r.fullUncompressedObject, err = r.ReadAll()
	return err
}

func (r *wrappedReader[T]) LastModified() time.Time {
	return r.fetcher.lastModified()
}

func (r *wrappedReader[T]) Size() int64 {
	return r.fetcher.size()
}

func newWrappedReader[T io.ReadCloser](
	fetcher *s3Fetcher, client s3iface.S3API, stats *stats.SourceStats, wrapper wrapper[T],
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
		wrapper:                wrapper,
	}, nil
}
