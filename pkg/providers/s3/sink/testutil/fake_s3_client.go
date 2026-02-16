package testutil

import (
	"io"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

type MockS3Client struct {
	BucketFiles map[string]map[string][]byte // bucket -> file -> data
}

func (m *MockS3Client) DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	if _, ok := m.BucketFiles[*input.Bucket]; !ok {
		return nil, xerrors.Errorf("bucket not found: %s", *input.Bucket)
	}
	if _, ok := m.BucketFiles[*input.Bucket][*input.Key]; !ok {
		return nil, xerrors.Errorf("file not found: %s", *input.Key)
	}
	delete(m.BucketFiles[*input.Bucket], *input.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func (m *MockS3Client) Upload(input *s3manager.UploadInput) (*s3manager.UploadOutput, error) {
	if _, ok := m.BucketFiles[*input.Bucket]; !ok {
		m.BucketFiles[*input.Bucket] = make(map[string][]byte)
	}
	for {
		buf := make([]byte, 1024)
		n, err := input.Body.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return &s3manager.UploadOutput{}, xerrors.Errorf("failed to read: %w", err)
		}
		m.BucketFiles[*input.Bucket][*input.Key] = append(m.BucketFiles[*input.Bucket][*input.Key], buf[:n]...)
	}

	return &s3manager.UploadOutput{}, nil
}

func NewMockS3Client() *MockS3Client {
	return &MockS3Client{BucketFiles: make(map[string]map[string][]byte)}
}
