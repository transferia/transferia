package delete

import (
	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

const maxBatchSize = 1000

type deleteS3Client interface {
	ListObjectsV2(input *aws_s3.ListObjectsV2Input) (*aws_s3.ListObjectsV2Output, error)
	DeleteObjects(input *aws_s3.DeleteObjectsInput) (*aws_s3.DeleteObjectsOutput, error)
}

type s3DeleteBatch struct {
	client  deleteS3Client
	bucket  string
	objects []*aws_s3.ObjectIdentifier
	deleted int
}

func NewS3DeleteBatch(client deleteS3Client, bucket string) *s3DeleteBatch {
	return &s3DeleteBatch{
		client:  client,
		bucket:  bucket,
		objects: make([]*aws_s3.ObjectIdentifier, 0),
		deleted: 0,
	}
}

func (b *s3DeleteBatch) Add(key string) error {
	b.objects = append(b.objects, &aws_s3.ObjectIdentifier{Key: aws.String(key)})
	if len(b.objects) < maxBatchSize {
		return nil
	}
	return b.Flush()
}

func (b *s3DeleteBatch) Flush() error {
	if len(b.objects) == 0 {
		return nil
	}

	res, err := b.client.DeleteObjects(&aws_s3.DeleteObjectsInput{
		Bucket: aws.String(b.bucket),
		Delete: &aws_s3.Delete{
			Objects: b.objects,
			Quiet:   aws.Bool(true),
		},
	})
	if err != nil {
		return xerrors.Errorf("unable to batch-delete %d objects: %w", len(b.objects), err)
	}
	if len(res.Errors) > 0 {
		first := res.Errors[0]
		return xerrors.Errorf(
			"failed to delete %d of %d objects: %s: %s",
			len(res.Errors), len(b.objects),
			aws.StringValue(first.Code), aws.StringValue(first.Message),
		)
	}

	b.deleted += len(b.objects)
	b.objects = nil
	return nil
}

func (b *s3DeleteBatch) Deleted() int {
	return b.deleted
}
