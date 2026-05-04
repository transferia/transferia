package s3raw

import (
	"context"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

type s3Fetcher struct {
	ctx    context.Context
	client s3iface.S3API
	bucket string // reference S3 bucket holding all the target objects in
	key    string // object key identifying an object in an S3 bucket

	objectSize            uint64 // the full size of the object stored in the S3 bucket
	lastModifiedTimestamp time.Time
}

func (f *s3Fetcher) headObjectInfo(input *aws_s3.HeadObjectInput) error {
	resp, err := f.client.HeadObjectWithContext(f.ctx, input)
	if err != nil {
		return xerrors.Errorf("unable to get HeadObjectWithContext, err: %w", err)
	}

	if resp.ContentLength == nil || *resp.ContentLength < 0 {
		return xerrors.Errorf("resp.ContentLength == nil || *resp.ContentLength < 0")
	}
	f.objectSize = uint64(*resp.ContentLength)

	if resp.LastModified == nil || (*resp.LastModified).IsZero() {
		return xerrors.Errorf("resp.LastModified == nil || (*resp.LastModified).IsZero()")
	}
	f.lastModifiedTimestamp = *resp.LastModified

	logger.Log.Debugf("S3 object s3://%s/%s has size %d lastModified timestamp is %v", f.bucket, f.key, f.objectSize, f.lastModifiedTimestamp)

	return nil
}

func (f *s3Fetcher) init() error {
	err := f.headObjectInfo(
		&aws_s3.HeadObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.key),
		},
	)
	if err != nil {
		return xerrors.Errorf("S3 HeadObject returned error, err: %w", err)
	}
	return nil
}

func (f *s3Fetcher) size() uint64 {
	return f.objectSize
}

func (f *s3Fetcher) lastModified() time.Time {
	return f.lastModifiedTimestamp
}

func (f *s3Fetcher) getObject(input *aws_s3.GetObjectInput) (*aws_s3.GetObjectOutput, error) {
	resp, err := f.client.GetObjectWithContext(f.ctx, input)
	if err != nil {
		return nil, xerrors.Errorf("unable to get ObjectWithContext, err: %w", err)
	}
	return resp, nil
}

func (f *s3Fetcher) makeReader() (io.ReadCloser, error) {
	resp, err := f.getObject(
		&aws_s3.GetObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.key),
		},
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to get Object, err: %w", err)
	}
	return resp.Body, nil
}

func newS3Fetcher(ctx context.Context, client s3iface.S3API, bucket string, key string) (*s3Fetcher, error) {
	result := &s3Fetcher{
		ctx:    ctx,
		client: client,
		bucket: bucket,
		key:    key,

		objectSize:            0,
		lastModifiedTimestamp: time.Time{},
	}
	err := result.init()
	return result, err
}
