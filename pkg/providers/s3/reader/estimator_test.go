package reader

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
)

// Reader function to return dummy S3RawReader with specified sizes
func dummyReaderF(sizes map[string]int64) readerCtorF {
	return func(ctx context.Context, filePath string) (s3raw.AbstractS3RawReader, error) {
		fileSize, exists := sizes[filePath]
		if !exists {
			return nil, xerrors.Errorf("file not found: %s", filePath)
		}
		return s3raw.NewFakeS3RawReader(fileSize), nil
	}
}

func TestEstimateTotalSize(t *testing.T) {
	tests := []struct {
		name          string
		files         []*s3.Object
		fileSizes     map[string]int64
		expectedSize  uint64
		expectedError error
	}{
		{
			name: "less than limit files",
			files: []*s3.Object{
				{Key: aws.String("file1")},
				{Key: aws.String("file2")},
			},
			fileSizes: map[string]int64{
				"file1": 100,
				"file2": 200,
			},
			expectedSize:  300,
			expectedError: nil,
		},
		{
			name: "more than limit files",
			files: func() []*s3.Object {
				files := []*s3.Object{}
				for i := 0; i < EstimateFilesLimit+5; i++ {
					files = append(files, &s3.Object{Key: aws.String(fmt.Sprintf("file%v", i))})
				}
				return files
			}(),
			fileSizes: func() map[string]int64 {
				sizes := map[string]int64{}
				for i := 0; i < EstimateFilesLimit+5; i++ {
					sizes[fmt.Sprintf("file%v", i)] = 100
				}
				return sizes
			}(),
			expectedSize:  uint64(100 * EstimateFilesLimit * (EstimateFilesLimit + 5) / EstimateFilesLimit),
			expectedError: nil,
		},
		{
			name: "error reading file",
			files: []*s3.Object{
				{Key: aws.String("file1")},
			},
			fileSizes:     map[string]int64{},
			expectedSize:  0,
			expectedError: xerrors.New("unable to estimate size"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, _, err := estimateTotalSize(context.Background(), logger.Log, tt.files, dummyReaderF(tt.fileSizes))

			require.Equal(t, tt.expectedSize, size)

			if tt.expectedError != nil {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expectedError.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
