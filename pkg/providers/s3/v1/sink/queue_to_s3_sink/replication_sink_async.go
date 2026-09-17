package queue_to_s3_sink

import (
	"context"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	s3_v1_model "github.com/transferia/transferia/pkg/providers/s3/v1/model"
	s3_v1_sink "github.com/transferia/transferia/pkg/providers/s3/v1/sink"
	s3_v1_sink_client "github.com/transferia/transferia/pkg/providers/s3/v1/sink/client"
	s3_v1_sink_writer "github.com/transferia/transferia/pkg/providers/s3/v1/sink/writer"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

type AsyncSink struct {
	cfg *s3_v1_model.S3Destination
	// rotator is temporarily of type *DefaultRotator until we implement other rotators.
	// In the future, this should be an interface populated via a factory.
	rotator     *DefaultRotator
	partitioner Partitioner
	serializer  s3_v1_model.SerializerConfig
	logger      log.Logger
	metrics     *stats.SinkerStats

	snapshotWriter *s3_v1_sink.SnapshotWriter
	s3Client       s3_v1_sink_client.S3Client

	offsetsToCommit []uint64

	//concurrentState is a wrapper around all the asyncronous variables that are needed for reqular rotation to work
	concurrentState *lifecycleState
}

var _ abstract.QueueToS3Sink = (*AsyncSink)(nil)

func (s *AsyncSink) Close() error {
	s.concurrentState.stopOnce.Do(func() { close(s.concurrentState.stopCh) })
	s.concurrentState.wg.Wait()
	s.concurrentState.mu.Lock()
	defer s.concurrentState.mu.Unlock()

	// Nothing was ever pushed, or the committer already closed the last file
	if s.snapshotWriter == nil {
		return nil
	}

	err := s.snapshotWriter.Close()
	// SnapshotWriter already closed, can happen when Close() is called while commiting new file
	if xerrors.Is(err, io.ErrClosedPipe) {
		return nil
	}
	return err
}

func (s *AsyncSink) regularRotationLoop() {
	ticker := time.NewTicker(s.rotator.cfg.Interval)
	defer ticker.Stop()
	defer s.concurrentState.wg.Done()

	for {
		select {
		case <-s.concurrentState.stopCh:
			return
		case <-ticker.C:
			s.runRegularCommit()
		}
	}
}

func (s *AsyncSink) runRegularCommit() {
	s.concurrentState.mu.Lock()
	defer s.concurrentState.mu.Unlock()

	// No file is open, skip rotation
	if s.snapshotWriter == nil {
		return
	}

	err := s.snapshotWriter.Close()
	// The next push opens a fresh file rather than writing into the closed one
	s.snapshotWriter = nil
	if err != nil {
		err = xerrors.Errorf("unable to commit file on wallclock rotation: %w", err)
	} else {
		s.logger.Info("committed file on wallclock rotation")
	}
	_ = s.sendStatus(s.concurrentState.pushCtx, s.concurrentState.resCh, err)
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

	rowFqtn := s3_v1_sink.RowFqtn(items[0].TableID())
	s.metrics.Table(rowFqtn, "rows", len(items))
	return nil
}

func (s *AsyncSink) initPipe(fileName string) error {
	pipeReader, pipeWriter := io.Pipe()

	batchSerializer, err := s3_v1_sink.CreateSerializer(s.serializer)
	if err != nil {
		return xerrors.Errorf("unable to create serializer with outputFormat: %s: %w", s.serializer.FormatName(), err)
	}
	writer := s3_v1_sink_writer.NewWriter(s.serializer.FormatEncoding(), pipeWriter)
	snapshotWriter, err := s3_v1_sink.NewsnapshotWriter(
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
				"debug-data-transfer.file-encoding": aws.String(string(s.serializer.FormatEncoding())),
				"debug-data-transfer.file-format":   aws.String(string(s.serializer.FormatName())),
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

	s.rotator.UpdateState(items)
	s.addOffsetsToCommit(items)
}

func (s *AsyncSink) processRotation(ctx context.Context, resCh chan<- abstract.AsyncPushResult, items []abstract.ChangeItem) {
	firstIdx := 0

	// s.snapshotWriter == nil during first push
	if s.snapshotWriter != nil {
		for i := range items {
			shouldRotate, err := s.rotator.ShouldRotate(items[:i+1])
			if err != nil {
				_ = s.sendStatus(ctx, resCh, err)
				return
			}
			if shouldRotate {
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
	// Reset Rotator state
	if err := s.rotator.ResetState(&items[firstIdx]); err != nil {
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
	s.asyncPush(ctx, resCh, items[firstIdx:])
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
	if len(items) == 0 {
		return
	}

	s.concurrentState.mu.Lock()
	defer s.concurrentState.mu.Unlock()

	// Latch the stream so that the wallclock committer can report between pushes
	s.concurrentState.pushCtx, s.concurrentState.resCh = ctx, errCh
	s.asyncPush(ctx, errCh, items)
}

// asyncPush holds the push logic and recurses, so it runs with mu already held
func (s *AsyncSink) asyncPush(ctx context.Context, errCh chan<- abstract.AsyncPushResult, items []abstract.ChangeItem) {
	// No file is open: before the first push, and after the regular committer closed the
	// previous one. Either way this batch starts a new file, whatever the rotator says.
	if s.snapshotWriter == nil {
		s.processRotation(ctx, errCh, items)
		return
	}

	shouldRotate, err := s.rotator.ShouldRotate(items)
	if err != nil {
		_ = s.sendStatus(ctx, errCh, err)
		return
	}
	if shouldRotate {
		s.processRotation(ctx, errCh, items)
		return
	}
	s.processBeforeRotation(ctx, errCh, items)
}

func NewReplicationAsyncSink(lgr log.Logger, cfg *s3_v1_model.S3Destination, mtrcs core_metrics.Registry) (*AsyncSink, error) {
	useReplace := false // for now cleanup is not supported for replication sink
	s3Client, err := s3_v1_sink_client.New(lgr, cfg.Bucket, cfg.Connection, cfg.PartSize, useReplace)
	if err != nil {
		return nil, xerrors.Errorf("unable to create s3 client: %w", err)
	}
	partitioner := NewPartitioner(cfg)
	rotator, err := NewDefaultRotator(cfg, partitioner)
	if err != nil {
		return nil, xerrors.Errorf("unable to create rotator: %w", err)
	}

	sink := &AsyncSink{
		logger:          lgr,
		metrics:         stats.NewSinkerStats(mtrcs),
		cfg:             cfg,
		rotator:         rotator,
		partitioner:     partitioner,
		s3Client:        s3Client,
		snapshotWriter:  nil, // We can not init writer in constructor, data from first received message is needed
		offsetsToCommit: make([]uint64, 0),
		serializer:      cfg.GetSerializer(),
		concurrentState: newLifecycleState(),
	}

	if rotator.cfg.IsRegularRotationEnabled {
		sink.concurrentState.wg.Add(1)
		go sink.regularRotationLoop()
	}
	return sink, nil
}
