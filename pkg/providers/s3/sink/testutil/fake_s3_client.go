package testutil

import (
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

type MockS3Client struct {
	BucketFiles        map[string]map[string][]byte
	ListPageSize       int
	DeleteObjectsCalls []*aws_s3.DeleteObjectsInput
}

func NewMockS3Client() *MockS3Client {
	return &MockS3Client{
		BucketFiles:        make(map[string]map[string][]byte),
		ListPageSize:       0,
		DeleteObjectsCalls: make([]*aws_s3.DeleteObjectsInput, 0),
	}
}

func (m *MockS3Client) DeleteObject(input *aws_s3.DeleteObjectInput) (*aws_s3.DeleteObjectOutput, error) {
	if _, ok := m.BucketFiles[*input.Bucket]; !ok {
		return nil, xerrors.Errorf("bucket not found: %s", *input.Bucket)
	}
	if _, ok := m.BucketFiles[*input.Bucket][*input.Key]; !ok {
		return nil, xerrors.Errorf("file not found: %s", *input.Key)
	}
	delete(m.BucketFiles[*input.Bucket], *input.Key)
	return &aws_s3.DeleteObjectOutput{}, nil
}

func (m *MockS3Client) DeleteObjects(input *aws_s3.DeleteObjectsInput) (*aws_s3.DeleteObjectsOutput, error) {
	m.DeleteObjectsCalls = append(m.DeleteObjectsCalls, input)
	if _, ok := m.BucketFiles[*input.Bucket]; !ok {
		return nil, xerrors.Errorf("bucket not found: %s", *input.Bucket)
	}
	for _, obj := range input.Delete.Objects {
		delete(m.BucketFiles[*input.Bucket], *obj.Key)
	}
	return &aws_s3.DeleteObjectsOutput{}, nil
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

func (m *MockS3Client) ListObjectsV2(input *aws_s3.ListObjectsV2Input) (*aws_s3.ListObjectsV2Output, error) {
	prefix := ""
	if input.Prefix != nil {
		prefix = *input.Prefix
	}
	files := m.BucketFiles[*input.Bucket]
	keys := make([]string, 0, len(files))
	for k := range files {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	startIdx := 0
	if input.ContinuationToken != nil {
		token := *input.ContinuationToken
		startIdx = sort.SearchStrings(keys, token)
	}
	keys = keys[startIdx:]

	pageSize := len(keys)
	if m.ListPageSize > 0 && m.ListPageSize < pageSize {
		pageSize = m.ListPageSize
	}
	page := keys[:pageSize]

	contents := make([]*aws_s3.Object, 0, len(page))
	for _, k := range page {
		contents = append(contents, &aws_s3.Object{
			Key:  aws.String(k),
			Size: aws.Int64(int64(len(files[k]))),
		})
	}

	isTruncated := pageSize < len(keys)
	var nextToken *string
	if isTruncated {
		nextToken = aws.String(keys[pageSize])
	}
	return &aws_s3.ListObjectsV2Output{
		Contents:              contents,
		IsTruncated:           aws.Bool(isTruncated),
		NextContinuationToken: nextToken,
	}, nil
}
