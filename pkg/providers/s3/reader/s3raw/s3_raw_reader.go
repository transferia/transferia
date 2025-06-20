//go:build !disable_s3_provider

package s3raw

import (
	"compress/gzip"
	"compress/zlib"
	"context"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/stats"
)

var _ io.ReaderAt = (*S3RawReader)(nil)

// S3RawReader is a wrapper holding is io.ReaderAt implementations.
// In the case of non gzipped files it will perform HTTP Range request to the s3 bucket.
// In the case of gzipped files it will read a chunk of the in memory decompressed file.
// New instances must be created with the NewS3Reader function.
// It is safe for concurrent use.
type S3RawReader struct {
	fetcher *s3Fetcher
	reader  io.ReaderAt
}

// ReadAt is a proxy call to the underlying reader implementation
func (r *S3RawReader) ReadAt(p []byte, off int64) (int, error) {
	read, err := r.reader.ReadAt(p, off)
	if err != nil && !xerrors.Is(err, io.EOF) {
		return read, xerrors.Errorf("failed to read from file: %s: %w", r.fetcher.key, err)
	}
	return read, err
}

// Size is a proxy call to the underlying fetcher method
func (r *S3RawReader) Size() int64 {
	return r.fetcher.size()
}

// Size is a proxy call to the underlying fetcher method
func (r *S3RawReader) LastModified() time.Time {
	return r.fetcher.lastModified()
}

func NewS3RawReader(ctx context.Context, client s3iface.S3API, downloader *s3manager.Downloader, bucket, key string, metrics *stats.SourceStats) (*S3RawReader, error) {
	fetcher, err := newS3Fetcher(ctx, client, bucket, key)
	if err != nil {
		return nil, xerrors.Errorf("failed to initialize new s3 fetcher for reader: %w", err)
	}

	var reader io.ReaderAt

	if strings.HasSuffix(key, ".gz") {
		reader, err = newWrappedReader(fetcher, downloader, metrics, gzip.NewReader)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new gzip reader: %w", err)
		}
	} else if strings.HasSuffix(key, ".zlib") {
		reader, err = newWrappedReader(fetcher, downloader, metrics, zlib.NewReader)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new zlib reader: %w", err)
		}
	} else {
		reader, err = newChunkedReader(fetcher, metrics)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new chunked reader: %w", err)
		}
	}

	s3Reader := &S3RawReader{
		fetcher: fetcher,
		reader:  reader,
	}
	return s3Reader, nil
}
