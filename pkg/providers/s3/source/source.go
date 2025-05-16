package source

import (
	"context"
	"fmt"
	"time"

	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/parsequeue"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/pusher"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ abstract.Source = (*S3Source)(nil)

type S3Source struct {
	ctx           context.Context
	cancel        func()
	srcModel      *s3.S3Source
	transferID    string
	client        s3iface.S3API
	logger        log.Logger
	metrics       *stats.SourceStats
	reader        reader.Reader
	cp            coordinator.Coordinator
	objectFetcher ObjectFetcher
	errCh         chan error
	pusher        pusher.Pusher
	inflightLimit int64
}

var (
	ReadProgressKey = "ReadProgressKey"
	CreationEvent   = "ObjectCreated:"
	TestEvent       = "s3:TestEvent"
)

func (s *S3Source) Run(sink abstract.AsyncSink) error {
	parseQ := parsequeue.New(s.logger, 10, sink, s.reader.ParsePassthrough, s.ack)
	return s.run(parseQ)
}

func (s *S3Source) run(parseQ *parsequeue.ParseQueue[pusher.Chunk]) error {
	defer s.metrics.Master.Set(0)
	backoffTimer := backoff.NewExponentialBackOff()
	backoffTimer.InitialInterval = time.Second * 10
	backoffTimer.MaxElapsedTime = 0
	backoffTimer.Reset()
	nextWaitDuration := backoffTimer.NextBackOff()

	currPusher := pusher.New(nil, parseQ, s.logger, s.inflightLimit)
	s.pusher = currPusher

	sqsFetcher, ok := s.objectFetcher.(*sqsSource)
	if ok {
		// enable heartbeat for message visibility
		go sqsFetcher.visibilityHeartbeat(s.errCh)
	}

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Stopping run")
			return nil
		case err := <-s.errCh:
			s.cancel() // after first error cancel ctx, so any other errors would be dropped, but not deadlocked
			return xerrors.Errorf("failed during run: %w", err)
		default:
		}
		s.metrics.Master.Set(1)
		objectList, err := s.objectFetcher.FetchObjects()
		if err != nil {
			return xerrors.Errorf("failed to get list of new objects: %w", err)
		}

		if len(objectList) == 0 {
			s.logger.Infof("No new file from s3 found, will wait %v", nextWaitDuration)
			time.Sleep(nextWaitDuration)
			nextWaitDuration = backoffTimer.NextBackOff()
			continue
		}
		backoffTimer.Reset()

		if err := util.ParallelDoWithContextAbort(s.ctx, len(objectList), int(s.srcModel.Concurrency), func(i int, ctx context.Context) error {
			singleObject := objectList[i]
			return s.reader.Read(ctx, singleObject.Name, currPusher)
		}); err != nil {
			return xerrors.Errorf("failed to read and push object: %w", err)
		}

		// reading did not result in issues but pushing might still fail
	}
}

func (s *S3Source) ack(chunk pusher.Chunk, pushSt time.Time, err error) {
	if err != nil {
		util.Send(s.ctx, s.errCh, err)
		return
	}

	// ack chunk and check if reading of file is done
	done, err := s.pusher.Ack(chunk)
	if err != nil {
		util.Send(s.ctx, s.errCh, err)
		return
	}

	if done {
		// commit this file
		err = s.objectFetcher.Commit(
			Object{
				Name:         chunk.FilePath,
				LastModified: time.Now(),
			},
		)
		if err != nil {
			util.Send(s.ctx, s.errCh, err)
			return
		}
	}

	s.logger.Info(
		fmt.Sprintf("Commit read changes done in %v", time.Since(pushSt)),
		log.Int("committed", len(chunk.Items)),
	)
	s.metrics.PushTime.RecordDuration(time.Since(pushSt))
}

func (s *S3Source) Stop() {
	s.cancel()
}

func NewSource(src *s3.S3Source, transferID string, logger log.Logger, registry metrics.Registry, cp coordinator.Coordinator) (abstract.Source, error) {
	sess, err := s3.NewAWSSession(logger, src.Bucket, src.ConnectionConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to create aws session: %w", err)
	}

	currMetrics := stats.NewSourceStats(registry)
	currReader, err := reader.New(src, logger, sess, currMetrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to create reader: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := aws_s3.New(sess)

	fetcher, err := NewObjectFetcher(ctx, client, logger, cp, transferID, currReader, sess, src)
	if err != nil {
		cancel()
		return nil, xerrors.Errorf("failed to initialize new object fetcher: %w", err)
	}

	return &S3Source{
		ctx:           ctx,
		cancel:        cancel,
		srcModel:      src,
		transferID:    transferID,
		client:        client,
		logger:        logger,
		metrics:       currMetrics,
		reader:        currReader,
		cp:            cp,
		objectFetcher: fetcher,
		errCh:         make(chan error, 1),
		pusher:        nil,
		inflightLimit: src.InflightLimit,
	}, nil
}
