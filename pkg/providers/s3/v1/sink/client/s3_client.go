package client

import (
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

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

func (s *s3ClientImpl) ListMultipartUploads(input *aws_s3.ListMultipartUploadsInput) (*aws_s3.ListMultipartUploadsOutput, error) {
	return s.client.ListMultipartUploads(input)
}

func (s *s3ClientImpl) ListParts(input *aws_s3.ListPartsInput) (*aws_s3.ListPartsOutput, error) {
	return s.client.ListParts(input)
}

func (s *s3ClientImpl) CompleteMultipartUpload(input *aws_s3.CompleteMultipartUploadInput) (*aws_s3.CompleteMultipartUploadOutput, error) {
	return s.client.CompleteMultipartUpload(input)
}

func (s *s3ClientImpl) AbortMultipartUpload(input *aws_s3.AbortMultipartUploadInput) (*aws_s3.AbortMultipartUploadOutput, error) {
	return s.client.AbortMultipartUpload(input)
}

func newS3ClientImpl(client *aws_s3.S3, uploader *s3manager.Uploader) S3Client {
	return &s3ClientImpl{
		client:   client,
		uploader: uploader,
	}
}
