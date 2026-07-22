package client

import (
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Client interface {
	Upload(input *s3manager.UploadInput) (*s3manager.UploadOutput, error)
	DeleteObject(input *aws_s3.DeleteObjectInput) (*aws_s3.DeleteObjectOutput, error)
	DeleteObjects(input *aws_s3.DeleteObjectsInput) (*aws_s3.DeleteObjectsOutput, error)
	ListObjectsV2(input *aws_s3.ListObjectsV2Input) (*aws_s3.ListObjectsV2Output, error)
	ListMultipartUploads(input *aws_s3.ListMultipartUploadsInput) (*aws_s3.ListMultipartUploadsOutput, error)
	ListParts(input *aws_s3.ListPartsInput) (*aws_s3.ListPartsOutput, error)
	CompleteMultipartUpload(input *aws_s3.CompleteMultipartUploadInput) (*aws_s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(input *aws_s3.AbortMultipartUploadInput) (*aws_s3.AbortMultipartUploadOutput, error)
}
