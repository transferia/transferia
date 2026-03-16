package queue_to_s3_sink

import (
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
	snapshot_sink "github.com/transferia/transferia/pkg/providers/s3/sink"
	s3_writer "github.com/transferia/transferia/pkg/providers/s3/sink/writer"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type AsyncSink struct {
	cfg         *s3_provider.S3Destination
	rotator     s3_provider.Rotator
	partitioner Partitioner
	logger      log.Logger
	metrics     *stats.SinkerStats

	snapshotWriter *snapshot_sink.SnapshotWriter
	s3Client       snapshot_sink.S3Client

	offsetsToCommit []uint64
}

var _ abstract.QueueToS3Sink = (*AsyncSink)(nil)

func (s *AsyncSink) Close() error {
	err := s.snapshotWriter.Close()
	// SnapshotWriter already closed, can happen when Close() is called while commiting new file
	if errors.Is(err, io.ErrClosedPipe) {
		return nil
	}
	return err
}

func (s *AsyncSink) pushBatch(items []*abstract.ChangeItem) error {
	writtenBytes, err := s.snapshotWriter.Write(items)
	if err != nil {
		return xerrors.Errorf("unable to write data: %w", err)
	}
	s.logger.Info(
		"wrote bytes",
		log.Int("input_length", len(items)),
		log.Int("written_bytes", writtenBytes),
	)

	rowFqtn := snapshot_sink.RowFqtn(items[0].TableID())
	s.metrics.Table(rowFqtn, "rows", len(items))
	return nil
}

func (s *AsyncSink) initPipe(fileName string) error {
	pipeReader, pipeWriter := io.Pipe()

	batchSerializer, err := snapshot_sink.CreateSerializer(s.cfg.OutputFormat, s.cfg.AnyAsString, nil)
	if err != nil {
		return xerrors.Errorf("unable to create serializer with outputFormat: %s: %w", s.cfg.OutputFormat, err)
	}
	writer := s3_writer.NewWriter(s.cfg.OutputEncoding, pipeWriter)
	snapshotWriter, err := snapshot_sink.NewsnapshotWriter(
		context.Background(),
		batchSerializer,
		writer,
		fileName,
	)
	if err != nil {
		return xerrors.Errorf("unable to create snapshot writer: %w", err)
	}

	s.snapshotWriter = snapshotWriter
	go func() {
		s.logger.Info("start uploading table part", log.String("file", fileName))

		uploadInput := &s3manager.UploadInput{
			Body:   pipeReader,
			Bucket: aws.String(s.cfg.Bucket),
			Key:    aws.String(fileName),
			Metadata: map[string]*string{
				"debug-data-transfer.file-encoding": aws.String(string(s.cfg.OutputEncoding)),
				"debug-data-transfer.file-format":   aws.String(string(s.cfg.OutputFormat)),
			},
		}

		res, err := s.s3Client.Upload(uploadInput)
		if err != nil {
			err = pipeReader.CloseWithError(xerrors.Errorf("unable to upload table part: %w", err))
		} else {
			err = pipeReader.Close()
		}
		snapshotWriter.FinishUpload(err)
		s.logger.Info("upload result", log.String("file", fileName), log.Any("res", res), log.Error(err))
	}()

	return nil
}

func (s *AsyncSink) processBeforeRotation(ctx context.Context, resCh chan<- abstract.AsyncPushResult, items []abstract.ChangeItem) {
	if len(items) == 0 {
		return
	}

	listOfLinks := make([]*abstract.ChangeItem, len(items))
	for i, item := range items {
		listOfLinks[i] = &item
	}

	if err := s.pushBatch(listOfLinks); err != nil {
		_ = s.sendStatus(ctx, resCh, err)
		return
	}

	s.addOffsetsToCommit(items)
}

func (s *AsyncSink) processRotation(ctx context.Context, resCh chan<- abstract.AsyncPushResult, items []abstract.ChangeItem) {
	firstIdx := 0

	// s.snapshotWriter == nil during first push
	if s.snapshotWriter != nil {
		for i := range items {
			if s.rotator.ShouldRotate(&items[i]) {
				firstIdx = i
				s.processBeforeRotation(ctx, resCh, items[:firstIdx])
				break
			}
		}

		// Signal that writing to previous file is finished
		if err := s.snapshotWriter.Close(); err != nil {
			_ = s.sendStatus(ctx, resCh, xerrors.Errorf("Current writer ended with error: %w", err))
			return
		}

		if !s.sendStatus(ctx, resCh, nil) { // Close() method was called
			return
		}
	}

	// Current item is the first one to be put in the next file
	// Update Rotator settings
	if err := s.rotator.UpdateState(&items[firstIdx]); err != nil {
		_ = s.sendStatus(ctx, resCh, err)
		return
	}

	// Start new upload with new file name
	filename, err := s.partitioner.ConstructKey(&items[firstIdx])
	if err != nil {
		_ = s.sendStatus(ctx, resCh, err)
		return
	}

	if err := s.initPipe(filename); err != nil {
		_ = s.sendStatus(ctx, resCh, err)
		return
	}

	// One batch can potentially contain changes for more than two files -> we need to check if second (third, fourth...) rotation is needed
	s.AsyncV2Push(ctx, resCh, items[firstIdx:])
}

func (s *AsyncSink) sendStatus(ctx context.Context, resCh chan<- abstract.AsyncPushResult, err error) bool {
	result := &abstract.QueueSourceAsyncPushResult{
		Result: abstract.QueueResult{
			Offsets: s.offsetsToCommit,
		},
		Err: err,
	}

	select {
	case resCh <- result:
		s.offsetsToCommit = make([]uint64, 0)
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *AsyncSink) addOffsetsToCommit(items []abstract.ChangeItem) {
	res := make([]uint64, len(items))
	for i := range items {
		res[i] = items[i].QueueMessageMeta.Offset
	}
	s.offsetsToCommit = append(s.offsetsToCommit, res...)
}

func (s *AsyncSink) AsyncV2Push(ctx context.Context, errCh chan<- abstract.AsyncPushResult, items []abstract.ChangeItem) {
	lastItem := items[len(items)-1]

	if s.rotator.ShouldRotate(&lastItem) {
		s.processRotation(ctx, errCh, items)
		return
	}
	s.processBeforeRotation(ctx, errCh, items)
}

func NewReplicationAsyncSink(lgr log.Logger, cfg *s3_provider.S3Destination, mtrcs metrics.Registry) (*AsyncSink, error) {
	sess, err := s3_provider.NewAWSSession(lgr, cfg.Bucket, cfg.ConnectionConfig())
	if err != nil {
		return nil, xerrors.Errorf("unable to create session to s3 bucket: %w", err)
	}

	s3Client := s3.New(sess)
	uploader := s3manager.NewUploader(sess)
	uploader.PartSize = cfg.PartSize

	return &AsyncSink{
		logger:          lgr,
		metrics:         stats.NewSinkerStats(mtrcs),
		cfg:             cfg,
		rotator:         cfg.Rotator,
		partitioner:     PartitionerFactory(NewPartitionerConfig(cfg)),
		s3Client:        snapshot_sink.NewS3ClientImpl(s3Client, uploader),
		snapshotWriter:  nil, // We can not init writer in constructor, data from first received message is needed
		offsetsToCommit: make([]uint64, 0),
	}, nil
}
