package queue_to_s3_sink

import (
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	s3recipe "github.com/transferia/transferia/pkg/providers/s3/v1/recipe"
	s3_v1_sink_client "github.com/transferia/transferia/pkg/providers/s3/v1/sink/client"
)

// failingUploadClient reads the whole body like a real uploader and then fails, as CompleteMultipartUpload can.
type failingUploadClient struct {
	s3_v1_sink_client.S3Client
}

func (c failingUploadClient) Upload(input *s3manager.UploadInput) (*s3manager.UploadOutput, error) {
	if _, err := io.Copy(io.Discard, input.Body); err != nil {
		return nil, xerrors.Errorf("unable to read body: %w", err)
	}
	return nil, xerrors.New("CompleteMultipartUpload failed")
}

// TestS3ReplicationSinkReportsUploadError checks that a failed upload is reported to the source instead of offsets
// committed for a file that was not stored.
func TestS3ReplicationSinkReportsUploadError(t *testing.T) {
	serializer := &s3_v1_model.CSVSerializerConfig{Encoding: s3_v1_model.NoEncoding}
	cfg := s3recipe.PrepareS3(t, "testS3SinkReplicationUploadError", serializer)
	cfg.RotatorConfig = s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{Interval: rotationInterval}}
	cfg.PartitionerConfig = s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}}

	currSink, err := NewReplicationAsyncSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)
	currSink.s3Client = failingUploadClient{S3Client: currSink.s3Client}

	resultCh := make(chan abstract.AsyncPushResult)
	firstResult := make(chan abstract.AsyncPushResult, 1)
	go func() {
		firstResult <- <-resultCh
		for range resultCh {
		}
	}()

	ctx := context.Background()
	currSink.AsyncV2Push(ctx, resultCh, generateQueueItems(100, 0, startTime))
	// A message of the next interval rotates the file, so the first file is committed.
	currSink.AsyncV2Push(ctx, resultCh, generateQueueItems(1, 100, startTime.Add(rotationInterval)))

	result := <-firstResult
	require.ErrorContains(t, result.GetError(), "CompleteMultipartUpload failed")
	_ = currSink.Close()
}
