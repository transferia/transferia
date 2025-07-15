package s3raw

import (
	"compress/gzip"
	"compress/zlib"
	"context"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/stats"
)

func NewS3RawReader(ctx context.Context, client s3iface.S3API, bucket, key string, metrics *stats.SourceStats) (S3RawReader, error) {
	fetcher, err := newS3Fetcher(ctx, client, bucket, key)
	if err != nil {
		return nil, xerrors.Errorf("failed to initialize new s3 fetcher for reader: %w", err)
	}

	var reader S3RawReader

	if strings.HasSuffix(key, ".gz") {
		reader, err = newWrappedReader(fetcher, client, metrics, gzip.NewReader)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new gzip reader: %w", err)
		}
	} else if strings.HasSuffix(key, ".zlib") {
		reader, err = newWrappedReader(fetcher, client, metrics, zlib.NewReader)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new zlib reader: %w", err)
		}
	} else {
		reader, err = newChunkedReader(fetcher, metrics)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new chunked reader: %w", err)
		}
	}

	return reader, nil
}
