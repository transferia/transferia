package sink

import (
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Client interface {
	Upload(input *s3manager.UploadInput) (*s3manager.UploadOutput, error)
	DeleteObject(input *aws_s3.DeleteObjectInput) (*aws_s3.DeleteObjectOutput, error)
	DeleteObjects(input *aws_s3.DeleteObjectsInput) (*aws_s3.DeleteObjectsOutput, error)
	ListObjectsV2(input *aws_s3.ListObjectsV2Input) (*aws_s3.ListObjectsV2Output, error)
}

type s3ClientImpl struct {
	client   *aws_s3.S3
	uploader *s3manager.Uploader
}

func (s *s3ClientImpl) Upload(input *s3manager.UploadInput) (*s3manager.UploadOutput, error) {
	return s.uploader.Upload(input)
}

func (s *s3ClientImpl) DeleteObject(input *aws_s3.DeleteObjectInput) (*aws_s3.DeleteObjectOutput, error) {
	return s.client.DeleteObject(input)
}

func (s *s3ClientImpl) DeleteObjects(input *aws_s3.DeleteObjectsInput) (*aws_s3.DeleteObjectsOutput, error) {
	return s.client.DeleteObjects(input)
}

func (s *s3ClientImpl) ListObjectsV2(input *aws_s3.ListObjectsV2Input) (*aws_s3.ListObjectsV2Output, error) {
	return s.client.ListObjectsV2(input)
}

func NewS3ClientImpl(client *aws_s3.S3, uploader *s3manager.Uploader) S3Client {
	return &s3ClientImpl{
		client:   client,
		uploader: uploader,
	}
}
