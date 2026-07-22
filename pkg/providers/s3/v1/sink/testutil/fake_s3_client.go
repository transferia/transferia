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

	// Replace-policy test support
	PendingUploads               []*aws_s3.MultipartUpload
	UploadParts                  map[string][]*aws_s3.Part // key: UploadId
	CompletedUploads             []*aws_s3.CompleteMultipartUploadInput
	AbortedUploads               []*aws_s3.AbortMultipartUploadInput
	DeleteObjectsErr             error // if set, DeleteObjects returns this error
	ListMultipartUploadsErr      error
	ListPartsErr                 error
	CompleteMultipartUploadErr   error
	ListMultipartUploadsPageSize int // 0 = no pagination
	ListPartsPageSize            int // 0 = no pagination
}

func NewMockS3Client() *MockS3Client {
	return &MockS3Client{
		BucketFiles:                  make(map[string]map[string][]byte),
		ListPageSize:                 0,
		DeleteObjectsCalls:           make([]*aws_s3.DeleteObjectsInput, 0),
		PendingUploads:               nil,
		UploadParts:                  make(map[string][]*aws_s3.Part),
		CompletedUploads:             nil,
		AbortedUploads:               nil,
		DeleteObjectsErr:             nil,
		ListMultipartUploadsErr:      nil,
		ListPartsErr:                 nil,
		CompleteMultipartUploadErr:   nil,
		ListMultipartUploadsPageSize: 0,
		ListPartsPageSize:            0,
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
	if m.DeleteObjectsErr != nil {
		return nil, m.DeleteObjectsErr
	}
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

func (m *MockS3Client) ListMultipartUploads(input *aws_s3.ListMultipartUploadsInput) (*aws_s3.ListMultipartUploadsOutput, error) {
	if m.ListMultipartUploadsErr != nil {
		return nil, m.ListMultipartUploadsErr
	}
	prefix := aws.ToString(input.Prefix)
	uploads := make([]*aws_s3.MultipartUpload, 0, len(m.PendingUploads))
	for _, u := range m.PendingUploads {
		if prefix == "" || strings.HasPrefix(aws.ToString(u.Key), prefix) {
			uploads = append(uploads, u)
		}
	}

	startIdx := 0
	if input.KeyMarker != nil || input.UploadIdMarker != nil {
		markerKey := aws.ToString(input.KeyMarker)
		markerID := aws.ToString(input.UploadIdMarker)
		for i, u := range uploads {
			if aws.ToString(u.Key) == markerKey && aws.ToString(u.UploadId) == markerID {
				startIdx = i + 1
				break
			}
		}
	}
	uploads = uploads[startIdx:]

	pageSize := len(uploads)
	if m.ListMultipartUploadsPageSize > 0 && m.ListMultipartUploadsPageSize < pageSize {
		pageSize = m.ListMultipartUploadsPageSize
	}
	page := uploads[:pageSize]
	isTruncated := pageSize < len(uploads)

	out := &aws_s3.ListMultipartUploadsOutput{
		Uploads:     page,
		IsTruncated: aws.Bool(isTruncated),
	}
	if isTruncated {
		last := page[len(page)-1]
		out.NextKeyMarker = last.Key
		out.NextUploadIdMarker = last.UploadId
	}
	return out, nil
}

func (m *MockS3Client) ListParts(input *aws_s3.ListPartsInput) (*aws_s3.ListPartsOutput, error) {
	if m.ListPartsErr != nil {
		return nil, m.ListPartsErr
	}
	parts := m.UploadParts[aws.ToString(input.UploadId)]
	startIdx := 0
	if input.PartNumberMarker != nil {
		marker := aws.ToInt64(input.PartNumberMarker)
		for i, p := range parts {
			if aws.ToInt64(p.PartNumber) == marker {
				startIdx = i + 1
				break
			}
		}
	}
	parts = parts[startIdx:]

	pageSize := len(parts)
	if m.ListPartsPageSize > 0 && m.ListPartsPageSize < pageSize {
		pageSize = m.ListPartsPageSize
	}
	page := parts[:pageSize]
	isTruncated := pageSize < len(parts)

	out := &aws_s3.ListPartsOutput{
		Parts:       page,
		IsTruncated: aws.Bool(isTruncated),
	}
	if isTruncated {
		out.NextPartNumberMarker = page[len(page)-1].PartNumber
	}
	return out, nil
}

func (m *MockS3Client) CompleteMultipartUpload(input *aws_s3.CompleteMultipartUploadInput) (*aws_s3.CompleteMultipartUploadOutput, error) {
	if m.CompleteMultipartUploadErr != nil {
		return nil, m.CompleteMultipartUploadErr
	}
	m.CompletedUploads = append(m.CompletedUploads, input)
	return &aws_s3.CompleteMultipartUploadOutput{}, nil
}

func (m *MockS3Client) AbortMultipartUpload(input *aws_s3.AbortMultipartUploadInput) (*aws_s3.AbortMultipartUploadOutput, error) {
	m.AbortedUploads = append(m.AbortedUploads, input)
	return &aws_s3.AbortMultipartUploadOutput{}, nil
}
