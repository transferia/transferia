package queue_to_s3_sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/format"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/s3sess"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	"github.com/transferia/transferia/pkg/providers/s3/v1/recipe"
)

var (
	topic     = "test_table_replication"
	partition = 1
)

func generateQueueItems(count int, startOffset uint64, commitTime time.Time) []abstract.ChangeItem {
	res := make([]abstract.ChangeItem, count)
	for i := range count {
		res[i] = abstract.MakeRawMessage(
			[]byte("stub"),
			topic,
			commitTime,
			topic,
			partition,
			int64(startOffset)+int64(i),
			[]byte("stub"),
		)
		res[i].FillQueueMessageMeta(topic, 1, startOffset+uint64(i), int(startOffset)+i)
	}
	return res
}

func readChannelOffsets(t *testing.T, errCh <-chan abstract.AsyncPushResult, expectedResultLens []int) <-chan struct{} {
	signalCh := make(chan struct{}, 1)

	go func() {
		defer func() { signalCh <- struct{}{} }()

		for idx := range expectedResultLens {
			res, ok := <-errCh
			require.True(t, ok)

			queueRes, ok := res.(*abstract.QueueSourceAsyncPushResult)
			require.True(t, ok)
			require.NoError(t, queueRes.Err)
			require.Equal(t, expectedResultLens[idx], len(queueRes.Result.Offsets))
		}
	}()

	return signalCh
}

func checkS3Files(t *testing.T, connection s3_model.ConnectionConfig, bucket string, key string, size int) {
	sess, err := s3sess.NewAWSSession(logger.Log, bucket, connection)
	require.NoError(t, err)
	s3Client := s3.New(sess)

	obj, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	require.NoError(t, err, fmt.Sprintf("expected key: %s", key))
	data, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	logger.Log.Infof("read data: %v", format.SizeInt(len(data)))
	require.True(t, len(data) > 0)
	unzipped, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	unzippedData, err := io.ReadAll(unzipped)
	require.NoError(t, err)
	logger.Log.Infof("unpack data: %v", format.SizeInt(len(unzippedData)))
	require.Len(t, unzippedData, size)
}

func TestS3ReplicationSink(t *testing.T) {
	t.Run("testS3SinkReplicationSimple", testS3SinkReplicationSimple)
	t.Run("testS3SinkReplicationSimpleCrash", testS3SinkReplicationSimpleCrash)
	t.Run("testS3SinkReplicationBorderBatch", testS3SinkReplicationBorderBatch)
	t.Run("testS3SinkReplicationBorderBatchCrash", testS3SinkReplicationBorderBatchCrash)
	t.Run("testS3SinkReplicationNoBlocking", testS3SinkReplicationNoBlocking)
}

func testS3SinkReplicationSimple(t *testing.T) {
	// Prepare sink configuration
	serializer := &s3_v1_model.CSVSerializerConfig{Encoding: s3_v1_model.GzipEncoding}
	cfg := s3recipe.PrepareS3(t, "testS3SinkReplicationSimple", serializer)
	cfg.RotatorConfig = s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{Interval: rotationInterval}}
	cfg.PartitionerConfig = s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}}

	ctx := context.Background()

	currSink, err := NewReplicationAsyncSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// Asynchronous read
	errCH := make(chan abstract.AsyncPushResult)
	sourceSignal := readChannelOffsets(t, errCH, []int{100})

	// Insert some messages
	firstBatch := generateQueueItems(100, 0, startTime)
	currSink.AsyncV2Push(ctx, errCH, firstBatch)
	// Send border message so one file is commited to S3 and offsets are sent back
	currSink.AsyncV2Push(ctx, errCH, generateQueueItems(1, 100, startTime.Add(rotationInterval)))

	// Wait for information about commited offsets to appear on source
	<-sourceSignal
	require.NoError(t, currSink.Close())

	// Check created file
	// Partitioner default [<prefix>]/<topic>/partition=<kafkaPartition>/<topic>+<kafkaPartition>+<startOffset>.<format>[.gz]
	expectedKey, err := currSink.partitioner.ConstructKey(&firstBatch[0])
	require.NoError(t, err)
	checkS3Files(t, cfg.Connection, cfg.Bucket, expectedKey, 8690)
}

func testS3SinkReplicationSimpleCrash(t *testing.T) {
	// Prepare sink configuration
	serializer := &s3_v1_model.CSVSerializerConfig{Encoding: s3_v1_model.GzipEncoding}
	cfg := s3recipe.PrepareS3(t, "testS3SinkReplicationSimpleCrash", serializer)
	cfg.RotatorConfig = s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{Interval: rotationInterval}}
	cfg.PartitionerConfig = s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}}

	ctx := context.Background()

	currSink, err := NewReplicationAsyncSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// Asynchronous read
	errCH := make(chan abstract.AsyncPushResult)

	// Insert some messages
	currSink.AsyncV2Push(ctx, errCH, generateQueueItems(100, 0, startTime))

	// Wait for information about commited offsets to appear
	require.NoError(t, currSink.Close())

	// Reinit sink
	cfg.RotatorConfig = s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{Interval: rotationInterval}}
	currSink, err = NewReplicationAsyncSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// Source read
	sourceSignal := readChannelOffsets(t, errCH, []int{130})

	// Insert batch that overlaps with previously commited file
	firstBatch := generateQueueItems(100, 0, startTime)
	currSink.AsyncV2Push(ctx, errCH, firstBatch)
	currSink.AsyncV2Push(ctx, errCH, generateQueueItems(30, 100, startTime))
	// Send border message so one file is commited to S3, commited file contains offsets from 50 to 99 included
	currSink.AsyncV2Push(ctx, errCH, generateQueueItems(1, 130, startTime.Add(rotationInterval)))

	// Wait for information about commited offsets to appear
	<-sourceSignal
	require.NoError(t, currSink.Close())

	// Check created file
	// Partitioner default [<prefix>]/<topic>/partition=<kafkaPartition>/<topic>+<kafkaPartition>+<startOffset>.<format>[.gz]
	expectedKey, err := currSink.partitioner.ConstructKey(&firstBatch[0])
	require.NoError(t, err)
	checkS3Files(t, cfg.Connection, cfg.Bucket, expectedKey, 11330) // Added another 30 change items
}

func testS3SinkReplicationBorderBatch(t *testing.T) {
	// Prepare sink configuration
	serializer := &s3_v1_model.CSVSerializerConfig{Encoding: s3_v1_model.GzipEncoding}
	cfg := s3recipe.PrepareS3(t, "testS3SinkReplicationBorderBatch", serializer)
	cfg.RotatorConfig = s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{Interval: rotationInterval}}
	cfg.PartitionerConfig = s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}}

	ctx := context.Background()

	currSink, err := NewReplicationAsyncSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// Asynchronous read
	errCH := make(chan abstract.AsyncPushResult)
	sourceSignal := readChannelOffsets(t, errCH, []int{100, 50})

	// Insert some messages
	firstBatch := generateQueueItems(50, 0, startTime)
	currSink.AsyncV2Push(ctx, errCH, firstBatch)
	// Insert batch that consists of messages that belong to two different files
	oldPart := generateQueueItems(50, 50, startTime)
	newPart := generateQueueItems(50, 100, startTime.Add(rotationInterval))
	borderBatch := make([]abstract.ChangeItem, 0)
	borderBatch = append(borderBatch, oldPart...)
	borderBatch = append(borderBatch, newPart...)
	currSink.AsyncV2Push(ctx, errCH, borderBatch)

	// Send another border item to commit second file
	currSink.AsyncV2Push(ctx, errCH, generateQueueItems(1, 150, startTime.Add(rotationInterval*2)))

	// Wait for information about commited offsets to appear on source
	<-sourceSignal
	require.NoError(t, currSink.Close())

	// Check created files
	// Partitioner default [<prefix>]/<topic>/partition=<kafkaPartition>/<topic>+<kafkaPartition>+<startOffset>.<format>[.gz]
	expectedKey, err := currSink.partitioner.ConstructKey(&firstBatch[0])
	require.NoError(t, err)
	checkS3Files(t, cfg.Connection, cfg.Bucket, expectedKey, 8690)

	expectedSecondKey, err := currSink.partitioner.ConstructKey(&newPart[0])
	require.NoError(t, err)
	checkS3Files(t, cfg.Connection, cfg.Bucket, expectedSecondKey, 4400) // 50 items
}

func testS3SinkReplicationBorderBatchCrash(t *testing.T) {
	// Prepare sink configuration
	serializer := &s3_v1_model.CSVSerializerConfig{Encoding: s3_v1_model.GzipEncoding}
	cfg := s3recipe.PrepareS3(t, "testS3SinkReplicationBorderBatchCrash", serializer)
	cfg.RotatorConfig = s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{Interval: rotationInterval}}
	cfg.PartitionerConfig = s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}}

	ctx := context.Background()

	currSink, err := NewReplicationAsyncSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// Asynchronous read
	errCH := make(chan abstract.AsyncPushResult)
	sourceSignal := readChannelOffsets(t, errCH, []int{100})

	// Insert some messages
	firstBatch := generateQueueItems(50, 0, startTime)
	currSink.AsyncV2Push(ctx, errCH, firstBatch)
	// Insert batch that consists of messages that belong to two different files
	oldPart := generateQueueItems(50, 50, startTime)
	newPart := generateQueueItems(20, 100, startTime.Add(rotationInterval))
	borderBatch := make([]abstract.ChangeItem, 0)
	borderBatch = append(borderBatch, oldPart...)
	borderBatch = append(borderBatch, newPart...)
	currSink.AsyncV2Push(ctx, errCH, borderBatch)

	// Wait for information about commited offsets to appear on source
	<-sourceSignal
	require.NoError(t, currSink.Close())

	// Reinit sink
	cfg.RotatorConfig = s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{Interval: rotationInterval}}
	currSink, err = NewReplicationAsyncSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// Source read
	errCh2 := make(chan abstract.AsyncPushResult)
	sourceSignal2 := readChannelOffsets(t, errCH, []int{50})

	// Insert items related to the second file (including those that were not commited previously)
	secondBatch := generateQueueItems(50, 100, startTime.Add(rotationInterval))
	currSink.AsyncV2Push(ctx, errCh2, secondBatch)
	// Send another border item to commit second file
	currSink.AsyncV2Push(ctx, errCH, generateQueueItems(1, 150, startTime.Add(rotationInterval*2)))

	// Wait for information about commited offsets to appear on source
	<-sourceSignal2
	require.NoError(t, currSink.Close())

	// Check created files
	// Partitioner default [<prefix>]/<topic>/partition=<kafkaPartition>/<topic>+<kafkaPartition>+<startOffset>.<format>[.gz]
	expectedKey, err := currSink.partitioner.ConstructKey(&firstBatch[0])
	require.NoError(t, err)
	checkS3Files(t, cfg.Connection, cfg.Bucket, expectedKey, 8690)

	expectedSecondKey, err := currSink.partitioner.ConstructKey(&secondBatch[0])
	require.NoError(t, err)
	checkS3Files(t, cfg.Connection, cfg.Bucket, expectedSecondKey, 4400) // 50 items
}

func testS3SinkReplicationNoBlocking(t *testing.T) {
	// Prepare sink configuration
	serializer := &s3_v1_model.CSVSerializerConfig{Encoding: s3_v1_model.GzipEncoding}
	cfg := s3recipe.PrepareS3(t, "testS3SinkReplicationNoBlocking", serializer)
	cfg.RotatorConfig = s3_v1_model.RotatorUnion{Default: &s3_v1_model.DefaultRotatorConfig{Interval: rotationInterval}}
	cfg.PartitionerConfig = s3_v1_model.PartitionerUnion{Default: &s3_v1_model.DefaultPartitionerConfig{}}

	ctx, cancel := context.WithCancel(context.Background())

	currSink, err := NewReplicationAsyncSink(logger.Log, cfg, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	// Asynchronous read
	errCH := make(chan abstract.AsyncPushResult)
	sourceSignal := make(chan struct{}, 1)

	// Insert some messages
	currSink.AsyncV2Push(ctx, errCH, generateQueueItems(100, 0, startTime))

	go func() {
		// Generate border item, call will be blocked because no one is reading from result chan
		currSink.AsyncV2Push(ctx, errCH, generateQueueItems(1, 100, startTime.Add(rotationInterval)))
		sourceSignal <- struct{}{}
	}()

	// Canceling context unblocks goroutine that tries to write result
	cancel()
	<-sourceSignal
}
