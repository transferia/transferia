package s3raw

import (
	"fmt"
	"io"
	"math"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
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
	start, end, returnErr := calcRange(int64(len(p)), off, int64(r.fetcher.size()))
	if returnErr != nil && !xerrors.Is(returnErr, io.EOF) {
		return 0, xerrors.Errorf("unable to calc range, err: %w", returnErr)
	}

	if end >= int64(r.fetcher.size()) {
		// reduce buffer size
		p = p[:end-start+1]
	}

	rng := fmt.Sprintf("bytes=%d-%d", start, end)

	logger.Log.Debugf("make a GetObject request for S3 object s3://%s/%s with range %s", r.fetcher.bucket, r.fetcher.key, rng)

	resp, err := r.fetcher.getObject(
		&aws_s3.GetObjectInput{
			Bucket: aws.String(r.fetcher.bucket),
			Key:    aws.String(r.fetcher.key),
			Range:  aws.String(rng),
		},
	)
	if err != nil {
		return 0, xerrors.Errorf("unable to GetObject, err: %w", err)
	}
	defer resp.Body.Close()

	n, err := io.ReadFull(resp.Body, p)

	r.stats.Size.Add(int64(n))
	if xerrors.Is(err, io.ErrUnexpectedEOF) {
		return n, io.EOF
	}

	expected := end - start + 1
	if err == nil || err == io.EOF {
		if int64(n) != expected {
			logger.Log.Infof("read %d bytes, but expected %d bytes for range %d-%d", n, expected, start, end)
		}
	}

	if err == nil && returnErr != nil {
		err = returnErr
	}

	return n, err
}

func (r *s3RawReader) startStreamReader() error {
	rawReader, err := r.fetcher.makeReader()
	if err != nil {
		return xerrors.Errorf("unable to make a reader for s3 raw reader, err: %w", err)
	}
	r.currentReader = rawReader

	return nil
}

func (r *s3RawReader) Read(p []byte) (int, error) {
	if r.currentReader == nil {
		if err := r.startStreamReader(); err != nil {
			return 0, xerrors.Errorf("unable to start streaming reader, err: %w", err)
		}
	}

	return r.currentReader.Read(p)
}

func (r *s3RawReader) Close() error {
	if r.currentReader == nil {
		return nil
	}

	// Take ownership of the reader and make Close idempotent.
	currentReader := r.currentReader
	r.currentReader = nil

	err := currentReader.Close()
	if err != nil {
		return err
	}

	return nil
}

func (r *s3RawReader) LastModified() time.Time {
	return r.fetcher.lastModified()
}

func (r *s3RawReader) Size() int64 {
	sz := r.fetcher.size()
	if sz > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(sz)
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
