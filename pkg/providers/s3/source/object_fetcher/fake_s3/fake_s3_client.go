//go:build !disable_s3_provider

package fake_s3

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/util/set"
)

type FakeS3Client struct {
	s3iface.S3API
	t     *testing.T
	files []*File
}

func (c *FakeS3Client) ListObjectsPagesWithContext(_ aws.Context, _ *s3.ListObjectsInput, callback func(*s3.ListObjectsOutput, bool) bool, _ ...request.Option) error {
	inputForCallback := s3.ListObjectsOutput{
		Contents: []*s3.Object{},
	}
	for _, currFile := range c.files {
		size := int64(len(currFile.Body))
		newObject := &s3.Object{
			Key:          &currFile.FileName,
			Size:         &size,
			LastModified: &currFile.LastModified,
		}
		inputForCallback.Contents = append(inputForCallback.Contents, newObject)
	}
	callback(&inputForCallback, true)
	return nil
}

func (c *FakeS3Client) validate() {
	uniqFiles := set.New[string]()
	for _, file := range c.files {
		if uniqFiles.Contains(file.FileName) {
			require.False(c.t, true)
		}
		uniqFiles.Add(file.FileName)
	}
}

func (c *FakeS3Client) SetFiles(newFiles []*File) {
	c.files = newFiles
	c.validate()
}

func (c *FakeS3Client) AddFile(newFile *File) {
	c.files = append(c.files, newFile)
	c.validate()
}

func NewFakeS3Client(t *testing.T) *FakeS3Client {
	//nolint:exhaustivestruct
	return &FakeS3Client{
		t:     t,
		files: make([]*File, 0),
	}
}
