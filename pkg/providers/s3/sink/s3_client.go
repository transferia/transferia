package sink

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Client interface {
	Upload(input *s3manager.UploadInput) (*s3manager.UploadOutput, error)
	DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
}

type s3ClientImpl struct {
	client   *s3.S3
	uploader *s3manager.Uploader
}

func (s *s3ClientImpl) Upload(input *s3manager.UploadInput) (*s3manager.UploadOutput, error) {
	return s.uploader.Upload(input)
}

func (s *s3ClientImpl) DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	return s.client.DeleteObject(input)
}

func NewS3ClientImpl(client *s3.S3, uploader *s3manager.Uploader) S3Client {
	return &s3ClientImpl{
		client:   client,
		uploader: uploader,
	}
}
