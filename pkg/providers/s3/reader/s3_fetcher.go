package reader

import (
	"context"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

type s3Fetcher struct {
	ctx                   context.Context
	client                s3iface.S3API
	bucket                string // reference S3 bucket holding all the target objects in
	key                   string // object key identifying an object in an S3 bucket
	objectSize            int64  // the full size of the object stored in the S3 bucket
	lastModifiedTimestamp time.Time
}

func (f *s3Fetcher) size() int64 {
	res, err := f.fetchSize()
	if err != nil {
		logger.Log.Warn("unable to fetch size", log.Error(err))
	}
	return res
}

func (f *s3Fetcher) fetchSize() (int64, error) {
	if f.objectSize < 0 {
		if err := f.headObjectInfo(&s3.HeadObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.key),
		}); err != nil {
			return -1, xerrors.Errorf("failed to head object %s: %w", f.key, err)
		}
		return f.objectSize, nil
	} else {
		return f.objectSize, nil
	}
}

func (f *s3Fetcher) lastModified() time.Time {
	res, err := f.fetchLastModified()
	if err != nil {
		logger.Log.Warn("unable to fetch lastModified timestamp", log.Error(err))
	}
	return res
}

func (f *s3Fetcher) fetchLastModified() (time.Time, error) {
	if f.lastModifiedTimestamp.IsZero() {
		if err := f.headObjectInfo(&s3.HeadObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.key),
		}); err != nil {
			return time.Now(), xerrors.Errorf("failed to head object %s: %w", f.key, err)
		}
		return f.lastModifiedTimestamp, nil

	} else {
		return f.lastModifiedTimestamp, nil
	}
}

func (f *s3Fetcher) headObjectInfo(input *s3.HeadObjectInput) error {
	client := f.client

	resp, err := client.HeadObjectWithContext(f.ctx, input)
	if err != nil {
		return xerrors.Errorf("unable to head obj: %w", err)
	}

	if resp.ContentLength == nil || *resp.ContentLength < 0 {
		return xerrors.Errorf("S3 object size is invalid: %d", resp.ContentLength)
	}

	f.objectSize = *resp.ContentLength

	if resp.LastModified == nil || (*resp.LastModified).IsZero() {
		return xerrors.Errorf("S3 object lastModified is invalid: %v", resp.LastModified)
	}

	f.lastModifiedTimestamp = *resp.LastModified
	logger.Log.Infof("S3 object s3://%s/%s has size %d lastModified timestamp is %v", f.bucket, f.key, f.objectSize, f.lastModifiedTimestamp)

	return nil
}

func calcRange(p []byte, off int64, totalSize int64) (int64, int64, error) {
	var err error
	if off < 0 {
		return 0, 0, xerrors.New("negative offset not allowed")
	}

	if totalSize <= 0 {
		return 0, 0, xerrors.New("unable to read form object with no size")
	}

	start := off
	end := off + int64(len(p)) - 1
	if end >= totalSize {
		// Clamp down the requested range.
		end = totalSize - 1
		err = io.EOF

		if end < start {
			// this occurs when offset is bigger than the total size of the object
			return 0, 0, xerrors.New("offset outside of possible range")
		}
		if end-start > int64(len(p)) {
			// should never occur
			return 0, 0, xerrors.New("covered range is bigger than full object size")
		}
	}

	return start, end, err
}

func (f *s3Fetcher) getObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	client := f.client

	resp, err := client.GetObjectWithContext(f.ctx, input)
	if err != nil {
		return nil, xerrors.Errorf("unable to get object: %w", err)
	}
	return resp, nil
}

func NewS3Fetcher(ctx context.Context, client s3iface.S3API, bucket string, key string) (*s3Fetcher, error) {
	return &s3Fetcher{
		ctx:                   ctx,
		client:                client,
		bucket:                bucket,
		key:                   key,
		objectSize:            -1,
		lastModifiedTimestamp: time.Time{},
	}, nil
}
