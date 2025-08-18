package s3raw

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/stats"
)

var _ S3RawReader = (*s3RawReader)(nil)

// s3RawReader is a reader that reads from S3.
type s3RawReader struct {
	fetcher *s3Fetcher
	stats   *stats.SourceStats

	currentReader io.ReadCloser
}

func (r *s3RawReader) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	_, err := r.fetcher.fetchSize()
	if err != nil {
		return 0, xerrors.Errorf("unable to fetch size: %w", err)
	}

	start, end, returnErr := calcRange(int64(len(p)), off, r.fetcher.objectSize)
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

func (r *s3RawReader) startStreamReader() error {
	rawReader, err := r.fetcher.makeReader()
	if err != nil {
		return xerrors.Errorf("failed to make stream reader for file %s: %w", r.fetcher.key, err)
	}
	r.currentReader = rawReader

	return nil
}

func (r *s3RawReader) Read(p []byte) (int, error) {
	if r.currentReader == nil {
		if err := r.startStreamReader(); err != nil {
			return 0, xerrors.Errorf("failed to start reader: %w", err)
		}
	}

	return r.currentReader.Read(p)
}

func (r *s3RawReader) Close() error {
	if r.currentReader != nil {
		return r.currentReader.Close()
	}
	return nil
}

func (r *s3RawReader) LastModified() time.Time {
	return r.fetcher.lastModified()
}

func (r *s3RawReader) Size() int64 {
	return r.fetcher.size()
}

func newS3RawReader(fetcher *s3Fetcher, stats *stats.SourceStats) (S3RawReader, error) {
	if fetcher == nil {
		return nil, xerrors.New("missing s3 fetcher for chunked reader")
	}

	if stats == nil {
		return nil, xerrors.New("missing stats for chunked reader")
	}

	reader := &s3RawReader{
		fetcher:       fetcher,
		stats:         stats,
		currentReader: nil,
	}
	return reader, nil
}
