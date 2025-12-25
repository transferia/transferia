package s3raw

import (
	"compress/gzip"
	"compress/zlib"
	"context"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/stats"
)

func NewS3RawReader(ctx context.Context, client s3iface.S3API, bucket, key string, metrics *stats.SourceStats) (S3RawReader, error) {
	fetcher, err := newS3Fetcher(ctx, client, bucket, key)
	if err != nil {
		return nil, xerrors.Errorf("failed to initialize new s3 fetcher for reader: %w", err)
	}

	// Check ContentEncoding from S3 metadata to avoid double decompression
	// If ContentEncoding is set, AWS SDK automatically decompresses, so we shouldn't apply wrapper
	headObj, err := client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to head object: %w", err)
	}
	var contentEncoding string
	if headObj.ContentEncoding != nil {
		contentEncoding = strings.ToLower(*headObj.ContentEncoding)
	}

	var reader S3RawReader

	// Only apply compression wrapper if:
	// 1. File has .gz/.zlib extension AND
	// 2. ContentEncoding is not set (AWS SDK won't auto-decompress)
	if strings.HasSuffix(key, ".gz") && (contentEncoding != "gzip" && contentEncoding != "x-gzip") { // A recipient SHOULD consider "x-gzip" to beequivalent to "gzip". see RFC 7230 4.2.3
		reader, err = newWrappedReader(fetcher, client, metrics, gzip.NewReader)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new gzip reader: %w", err)
		}
	} else if strings.HasSuffix(key, ".zlib") && contentEncoding != "deflate" { // deflate is content encoding for zlib. see RFC 7230 4.2.2
		reader, err = newWrappedReader(fetcher, client, metrics, zlib.NewReader)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new zlib reader: %w", err)
		}
	} else {
		reader, err = newS3RawReader(fetcher, metrics)
		if err != nil {
			return nil, xerrors.Errorf("failed to initialize new chunked reader: %w", err)
		}
	}

	return reader, nil
}
