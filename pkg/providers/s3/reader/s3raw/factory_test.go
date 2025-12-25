package s3raw

import (
	"compress/gzip"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/stats"
)

type mockS3API struct {
	s3iface.S3API

	headObjectF func(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
}

func (m *mockS3API) HeadObjectWithContext(ctx context.Context, input *s3.HeadObjectInput, opts ...request.Option) (*s3.HeadObjectOutput, error) {
	if m.headObjectF != nil {
		return m.headObjectF(input)
	}
	return nil, xerrors.New("not implemented")
}

type testCase struct {
	name            string
	bucket          string
	key             string
	contentEncoding *string
	expectedReader  S3RawReader
	expectedError   error
}

func TestNewS3RawReader(t *testing.T) {
	testData := make(map[string]map[string]*string)
	mockS3API := &mockS3API{
		headObjectF: func(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			if _, ok := testData[*input.Bucket]; !ok {
				testData[*input.Bucket] = make(map[string]*string)
			}
			if _, ok := testData[*input.Bucket][*input.Key]; !ok {
				return nil, xerrors.New("key not found")
			}
			return &s3.HeadObjectOutput{
				ContentEncoding: testData[*input.Bucket][*input.Key],
			}, nil
		},
	}

	for _, test := range []testCase{
		{
			name:            "gzip-key auto decompression should be raw reader",
			key:             "test-key.gz",
			bucket:          "test-bucket-gzip",
			contentEncoding: aws.String("gzip"),
			expectedReader:  &s3RawReader{},
			expectedError:   nil,
		},
		{
			name:            "x-gzip-key auto decompression should be raw reader",
			key:             "test-key.x-gzip",
			bucket:          "test-bucket-x-gzip",
			contentEncoding: aws.String("x-gzip"),
			expectedReader:  &s3RawReader{},
			expectedError:   nil,
		},
		{
			name:            "deflate auto decompression should be raw reader",
			key:             "other-key.zlib",
			bucket:          "other-bucket",
			contentEncoding: aws.String("deflate"),
			expectedReader:  &s3RawReader{},
			expectedError:   nil,
		},
		{
			name:            "bad ContentEncoding header and gz suffix should be gzip reader",
			key:             "third-key.gz",
			bucket:          "third-bucket-with-bad-encoding",
			contentEncoding: aws.String("bad-encoding"),
			expectedReader:  &wrappedReader[*gzip.Reader]{},
			expectedError:   nil,
		},
		{
			name:            "zlib suffix without content encoding should be zlib reader",
			key:             "zlib-key.zlib",
			bucket:          "zlib-without-content-encoding",
			contentEncoding: nil,
			expectedReader:  &wrappedReader[io.ReadCloser]{},
			expectedError:   nil,
		},
		{
			name:            "gz suffix without content encoding should be gz reader",
			key:             "gz-key.gz",
			bucket:          "gz-without-content-encoding",
			contentEncoding: nil,
			expectedReader:  &wrappedReader[*gzip.Reader]{},
			expectedError:   nil,
		},
		{
			name:            "without content encoding and suffix should be raw reader",
			key:             "without-content-encoding-key",
			bucket:          "without-content-encoding-and-suffix",
			contentEncoding: nil,
			expectedReader:  &s3RawReader{},
			expectedError:   nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			testData[test.bucket] = map[string]*string{test.key: test.contentEncoding}
			metrics := stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts()))
			reader, err := NewS3RawReader(context.Background(), mockS3API, test.bucket, test.key, metrics)
			require.NoError(t, err)
			require.IsType(t, test.expectedReader, reader)
		})
	}
}
