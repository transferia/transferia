//go:build !disable_s3_provider

package s3raw

import (
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/stats"
)

var _ io.ReaderAt = (*chunkedReader)(nil)

type chunkedReader struct {
	fetcher *s3Fetcher
	stats   *stats.SourceStats
}

func (r *chunkedReader) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	_, err := r.fetcher.fetchSize()
	if err != nil {
		return 0, xerrors.Errorf("unable to fetch size: %w", err)
	}

	start, end, returnErr := calcRange(p, off, r.fetcher.objectSize)
	if returnErr != nil && !xerrors.Is(returnErr, io.EOF) {
		return 0, xerrors.Errorf("unable to calculate new read range for file %s: %w", r.fetcher.key, returnErr)
	}

	if end >= r.fetcher.objectSize {
		// reduce buffer size
		p = p[:end-start+1]
	}

	rng := fmt.Sprintf("bytes=%d-%d", start, end)

	logger.Log.Debugf("make a GetObject request for S3 object s3://%s/%s with range %s", r.fetcher.bucket, r.fetcher.key, rng)

	resp, err := r.fetcher.getObject(&s3.GetObjectInput{
		Bucket: aws.String(r.fetcher.bucket),
		Key:    aws.String(r.fetcher.key),
		Range:  aws.String(rng),
	})
	if err != nil {
		return 0, xerrors.Errorf("S3 GetObject error: %w", err)
	}
	defer resp.Body.Close()

	n, err := io.ReadFull(resp.Body, p)

	r.stats.Size.Add(int64(n))
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return n, io.EOF
	}

	if (err == nil || err == io.EOF) && int64(n) != *resp.ContentLength {
		logger.Log.Infof("read %d bytes, but the content-length was %d\n", n, resp.ContentLength)
	}

	if err == nil && returnErr != nil {
		err = returnErr
	}

	return n, err
}

func newChunkedReader(fetcher *s3Fetcher, stats *stats.SourceStats) (io.ReaderAt, error) {
	if fetcher == nil {
		return nil, xerrors.New("missing s3 fetcher for chunked reader")
	}

	if stats == nil {
		return nil, xerrors.New("missing stats for chunked reader")
	}

	reader := &chunkedReader{
		fetcher: fetcher,
		stats:   stats,
	}
	return reader, nil
}
