package fake_s3

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/set"
)

type FakeS3Client struct {
	s3iface.S3API
	t     *testing.T
	files []*File
}

func (c *FakeS3Client) ListObjects(_ *aws_s3.ListObjectsInput) (*aws_s3.ListObjectsOutput, error) {
	inputForCallback := &aws_s3.ListObjectsOutput{
		Contents: []*aws_s3.Object{},
	}
	for _, currFile := range c.files {
		size := int64(len(currFile.Body))
		newObject := &aws_s3.Object{
			Key:          &currFile.FileName,
			Size:         &size,
			LastModified: &currFile.LastModified,
		}
		inputForCallback.Contents = append(inputForCallback.Contents, newObject)
	}
	return inputForCallback, nil
}

func (c *FakeS3Client) ListObjectsPagesWithContext(_ aws.Context, _ *aws_s3.ListObjectsInput, callback func(*aws_s3.ListObjectsOutput, bool) bool, _ ...request.Option) error {
	inputForCallback, _ := c.ListObjects(nil)
	callback(inputForCallback, true)
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

func (c *FakeS3Client) GetFile(fileName string) (*File, error) {
	for _, file := range c.files {
		if file.FileName == fileName {
			return file, nil
		}
	}

	return nil, xerrors.Errorf("fake s3 file %q not found", fileName)
}

// PickFileForRead returns the only file when the fake holds one object; otherwise selects by S3 key.
func (c *FakeS3Client) PickFileForRead(objectKey string) (*File, error) {
	switch len(c.files) {
	case 0:
		return nil, xerrors.New("fake s3 has no files")
	case 1:
		return c.files[0], nil
	default:
		return c.GetFile(objectKey)
	}
}

func (c *FakeS3Client) GetSingleFile() (*File, error) {
	switch len(c.files) {
	case 0:
		return nil, xerrors.New("fake s3 has no files")
	case 1:
		return c.files[0], nil
	default:
		return nil, xerrors.Errorf("fake s3 has %d files, expected 1", len(c.files))
	}
}

func NewFakeS3Client(t *testing.T) *FakeS3Client {
	//nolint:exhaustivestruct
	return &FakeS3Client{
		t:     t,
		files: make([]*File, 0),
	}
}
